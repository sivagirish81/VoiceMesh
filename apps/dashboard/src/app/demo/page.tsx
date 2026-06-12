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
  const [transcript, setTranscript] = useState("");
  const [response, setResponse] = useState("");
  const [events, setEvents] = useState<PipelineEvent[]>([]);
  const [pipeline, setPipeline] = useState<PipelineView>({
    stage: "transport", corked: false, queue_depths: {},
  });
  const websocket = useRef<WebSocket | null>(null);
  const stream = useRef<MediaStream | null>(null);
  const audioContext = useRef<AudioContext | null>(null);
  const processor = useRef<ScriptProcessorNode | null>(null);
  const playbackAt = useRef(0);

  const playPcm = useCallback((base64: string, sampleRate: number) => {
    const context = audioContext.current ?? new AudioContext();
    audioContext.current = context;
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
    playbackAt.current = startAt + buffer.duration;
  }, []);

  async function startCall() {
    const id = crypto.randomUUID();
    setCallId(id);
    setEvents([]);
    setTranscript("");
    setResponse("");
    const socket = new WebSocket(`${WS_URL}/ws/calls/${id}`);
    websocket.current = socket;
    socket.onopen = async () => {
      setConnected(true);
      const media = await navigator.mediaDevices.getUserMedia({
        audio: {channelCount: 1, echoCancellation: true, noiseSuppression: true},
      });
      stream.current = media;
      const context = new AudioContext();
      audioContext.current = context;
      await context.resume();
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
      const data = JSON.parse(message.data);
      if (data.type === "transcript.final") setTranscript(data.text);
      if (data.type === "llm.token") setResponse((current) => current + data.text);
      if (data.type === "audio.chunk") playPcm(data.audio, data.sample_rate);
      if (data.type === "pipeline.event") {
        setEvents((current) => [...current.slice(-199), data.event]);
        setPipeline(data.state);
      }
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
      setConnected(false);
      setRecording(false);
    };
  }

  function stopCall() {
    if (websocket.current?.readyState === WebSocket.OPEN) {
      websocket.current.send(JSON.stringify({type: "audio.end_turn"}));
      setTimeout(() => websocket.current?.send(JSON.stringify({type: "call.end"})), 100);
    }
    processor.current?.disconnect();
    stream.current?.getTracks().forEach((track) => track.stop());
    setRecording(false);
  }

  useEffect(() => () => {
    websocket.current?.close();
    stream.current?.getTracks().forEach((track) => track.stop());
    audioContext.current?.close();
  }, []);

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
          <button className="button danger" disabled={!recording} onClick={stopCall}>End call</button>
        </div>
      </section>
      <div className="grid three">
        <div className="card"><h3>Transport</h3><div className={`status ${connected ? "" : "idle"}`}>{connected ? "connected" : "idle"}</div></div>
        <div className="card"><h3>Microphone</h3><div className={`status ${recording ? "" : "idle"}`}>{recording ? "streaming PCM" : "stopped"}</div></div>
        <div className="card"><h3>Backpressure</h3><div className={`status ${pipeline.corked ? "corked" : ""}`}>{pipeline.corked ? "corked" : "uncorked"}</div></div>
      </div>
      <div className="grid two">
        <div className="card">
          <h3>Final transcript</h3>
          <div className="transcript">{transcript || "Speak naturally; silence closes the turn."}</div>
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

