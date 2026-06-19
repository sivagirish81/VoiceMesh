"use client";

import {useCallback, useEffect, useRef, useState} from "react";
import {DemoControls} from "@/components/DemoControls";
import {EventFeed} from "@/components/EventFeed";
import {PipelineState} from "@/components/PipelineState";
import {PipelineEvent, WS_URL} from "@/lib/api";

type PipelineView = {
  stage: string;
  corked: boolean;
  cork_reason?: string | null;
  queue_depths: Record<string, number>;
  vad?: {
    provider?: string;
    state?: string;
    decision?: string;
    energy?: number | null;
    noise_floor?: number | null;
    probability?: number | null;
    sample_rate?: number;
    frame_duration_ms?: number;
    speech_duration_ms?: number;
    total_duration_ms?: number;
    speech_frame_ratio?: number;
  };
};

type MicDebug = {
  echoCancellation?: boolean;
  noiseSuppression?: boolean;
  autoGainControl?: boolean;
  sampleRate?: number;
  channelCount?: number;
};

function floatToInt16(input: Float32Array): ArrayBuffer {
  const output = new Int16Array(input.length);
  for (let index = 0; index < input.length; index += 1) {
    const sample = Math.max(-1, Math.min(1, input[index]));
    output[index] = sample < 0 ? sample * 0x8000 : sample * 0x7fff;
  }
  return output.buffer;
}

export default function DemoPage() {
  const [callId, setCallId] = useState("");
  const [connected, setConnected] = useState(false);
  const [recording, setRecording] = useState(false);
  const [partialTranscript, setPartialTranscript] = useState("");
  const [transcript, setTranscript] = useState("");
  const [response, setResponse] = useState("");
  const [events, setEvents] = useState<PipelineEvent[]>([]);
  const [micDebug, setMicDebug] = useState<MicDebug>({});
  const [ignoredNoiseTurns, setIgnoredNoiseTurns] = useState(0);
  const [pipeline, setPipeline] = useState<PipelineView>({
    stage: "transport", corked: false, queue_depths: {},
  });
  const websocket = useRef<WebSocket | null>(null);
  const stream = useRef<MediaStream | null>(null);
  const audioContext = useRef<AudioContext | null>(null);
  const processor = useRef<ScriptProcessorNode | null>(null);
  const playbackAt = useRef(0);
  const playbackSources = useRef<Set<AudioBufferSourceNode>>(new Set());

  const stopPlayback = useCallback(() => {
    playbackSources.current.forEach((source) => {
      try {
        source.stop();
      } catch {
        // The source may already have ended. Hang-up should still be best-effort immediate.
      }
      source.disconnect();
    });
    playbackSources.current.clear();
    playbackAt.current = 0;
  }, []);

  const stopCapture = useCallback(() => {
    if (processor.current) {
      processor.current.onaudioprocess = null;
      processor.current.disconnect();
      processor.current = null;
    }
    stream.current?.getTracks().forEach((track) => track.stop());
    stream.current = null;
  }, []);

  const playPcm = useCallback((base64: string, sampleRate: number) => {
    const context = audioContext.current ?? new AudioContext();
    audioContext.current = context;
    void context.resume();
    const binary = atob(base64);
    const bytes = new Uint8Array(binary.length);
    for (let index = 0; index < binary.length; index += 1) bytes[index] = binary.charCodeAt(index);
    const samples = new Int16Array(bytes.buffer);
    const buffer = context.createBuffer(1, samples.length, sampleRate);
    const channel = buffer.getChannelData(0);
    for (let index = 0; index < samples.length; index += 1) channel[index] = samples[index] / 32768;
    const source = context.createBufferSource();
    source.buffer = buffer;
    source.connect(context.destination);
    const startAt = Math.max(context.currentTime + 0.03, playbackAt.current);
    source.start(startAt);
    playbackSources.current.add(source);
    source.onended = () => {
      playbackSources.current.delete(source);
      source.disconnect();
    };
    playbackAt.current = startAt + buffer.duration;
  }, []);

  async function startCall() {
    stopPlayback();
    stopCapture();
    websocket.current?.close();
    websocket.current = null;
    const id = crypto.randomUUID();
    setCallId(id);
    setEvents([]);
    setPartialTranscript("");
    setTranscript("");
    setResponse("");
    setIgnoredNoiseTurns(0);
    setMicDebug({});
    setPipeline({stage: "transport", corked: false, queue_depths: {}});
    const socket = new WebSocket(`${WS_URL}/ws/calls/${id}`);
    websocket.current = socket;
    socket.onopen = async () => {
      if (websocket.current !== socket) return;
      setConnected(true);
      const media = await navigator.mediaDevices.getUserMedia({
        audio: {
          channelCount: 1,
          echoCancellation: true,
          noiseSuppression: true,
          autoGainControl: true,
        },
      });
      stream.current = media;
      const context = new AudioContext();
      audioContext.current = context;
      await context.resume();
      const settings = media.getAudioTracks()[0]?.getSettings() as MediaTrackSettings & {
        autoGainControl?: boolean;
        echoCancellation?: boolean;
        noiseSuppression?: boolean;
      };
      setMicDebug({
        echoCancellation: settings?.echoCancellation,
        noiseSuppression: settings?.noiseSuppression,
        autoGainControl: settings?.autoGainControl,
        sampleRate: settings?.sampleRate ?? context.sampleRate,
        channelCount: settings?.channelCount ?? 1,
      });
      const source = context.createMediaStreamSource(media);
      const node = context.createScriptProcessor(2048, 1, 1);
      processor.current = node;
      socket.send(JSON.stringify({type: "audio.config", sample_rate: context.sampleRate, channels: 1}));
      node.onaudioprocess = (event) => {
        if (socket.readyState === WebSocket.OPEN) {
          socket.send(floatToInt16(event.inputBuffer.getChannelData(0)));
        }
      };
      source.connect(node);
      node.connect(context.destination);
      setRecording(true);
    };
    socket.onmessage = (message) => {
      if (websocket.current !== socket) return;
      const data = JSON.parse(message.data);
      if (data.type === "transcript.partial") {
        setPartialTranscript((current) => current + data.delta);
      }
      if (data.type === "transcript.final") {
        setTranscript(data.text);
        setPartialTranscript("");
      }
      if (data.type === "llm.token") setResponse((current) => current + data.text);
      if (data.type === "audio.chunk") playPcm(data.audio, data.sample_rate);
      if (data.type === "pipeline.event") {
        setEvents((current) => [...current.slice(-199), data.event]);
        setPipeline(data.state);
      }
      if (data.type === "vad.noise_turn_ignored") {
        setIgnoredNoiseTurns((current) => current + 1);
      }
      if (data.type === "pipeline.state") setPipeline(data.state);
      if (data.type === "pipeline.corked" || data.type === "pipeline.uncorked") {
        setPipeline((current) => ({
          ...current,
          corked: data.corked,
          cork_reason: data.reason,
          queue_depths: data.queue_depths,
        }));
      }
    };
    socket.onclose = () => {
      if (websocket.current !== socket) return;
      setConnected(false);
      setRecording(false);
      websocket.current = null;
    };
  }

  function stopCall() {
    const socket = websocket.current;
    websocket.current = null;
    if (socket?.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify({type: "call.end"}));
      socket.close(1000, "call ended by user");
    } else if (socket?.readyState === WebSocket.CONNECTING) {
      socket.close(1000, "call ended by user");
    }
    stopCapture();
    stopPlayback();
    void audioContext.current?.close();
    audioContext.current = null;
    setConnected(false);
    setRecording(false);
    setCallId("");
    setPartialTranscript("");
    setPipeline({stage: "transport", corked: false, queue_depths: {}});
  }

  useEffect(() => () => {
    websocket.current?.close();
    stopCapture();
    stopPlayback();
    audioContext.current?.close();
  }, [stopCapture, stopPlayback]);

  return (
    <div className="stack">
      <section className="row" style={{alignItems: "flex-end"}}>
        <div>
          <div className="eyebrow">Browser microphone → real providers → browser audio</div>
          <h1 style={{fontSize: 46, marginBottom: 8}}>Live reliability console</h1>
          <div className="mono muted">call_id: {callId || "not started"}</div>
        </div>
        <div className="actions">
          <button className="button primary" disabled={connected} onClick={startCall}>Start microphone</button>
          <button className="button danger" disabled={!recording && !connected} onClick={stopCall}>End call</button>
        </div>
      </section>
      <div className="grid three">
        <div className="card"><h3>Transport</h3><div className={`status ${connected ? "" : "idle"}`}>{connected ? "connected" : "idle"}</div></div>
        <div className="card"><h3>Microphone</h3><div className={`status ${recording ? "" : "idle"}`}>{recording ? "streaming PCM" : "stopped"}</div></div>
        <div className="card"><h3>Backpressure</h3><div className={`status ${pipeline.corked ? "corked" : ""}`}>{pipeline.corked ? "corked" : "uncorked"}</div></div>
      </div>
      <div className="grid two">
        <div className="card">
          <h3>Browser mic controls</h3>
          <div className="mono muted">
            echoCancellation: {String(micDebug.echoCancellation ?? "unknown")}<br />
            noiseSuppression: {String(micDebug.noiseSuppression ?? "unknown")}<br />
            autoGainControl: {String(micDebug.autoGainControl ?? "unknown")}<br />
            sampleRate: {micDebug.sampleRate ?? "unknown"} / channels: {micDebug.channelCount ?? "unknown"}
          </div>
        </div>
        <div className="card">
          <h3>VAD debug</h3>
          <div className="mono muted">
            provider: {pipeline.vad?.provider ?? "unknown"} / state: {pipeline.vad?.state ?? "idle"}<br />
            decision: {pipeline.vad?.decision ?? "unknown"} / ignored noise turns: {ignoredNoiseTurns}<br />
            energy: {pipeline.vad?.energy?.toFixed(4) ?? "n/a"} / noise_floor: {pipeline.vad?.noise_floor?.toFixed(4) ?? "n/a"}<br />
            speech_ms: {pipeline.vad?.speech_duration_ms?.toFixed(0) ?? "0"} / ratio: {pipeline.vad?.speech_frame_ratio?.toFixed(2) ?? "0.00"}
          </div>
        </div>
      </div>
      <div className="grid two">
        <div className="card">
          <h3>Streaming transcript</h3>
          <div className="transcript">
            {partialTranscript || transcript || "Speak naturally; partial text appears before silence closes the turn."}
          </div>
        </div>
        <div className="card">
          <h3>Streaming response</h3>
          <div className="transcript response">{response || "The model response will stream here."}</div>
        </div>
      </div>
      <PipelineState stage={pipeline.stage} corked={pipeline.corked} corkReason={pipeline.cork_reason} queueDepths={pipeline.queue_depths} />
      <DemoControls callId={callId} />
      <EventFeed events={events} />
    </div>
  );
}
