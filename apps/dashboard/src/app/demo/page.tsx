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
  active_response_id?: string | null;
  barge_in?: {
    state?: string;
    active_response_id?: string | null;
    candidate_id?: string | null;
    last_semantic?: string | null;
    playback?: {
      last_played_sequence?: number;
      played_audio_ms?: number;
    } | null;
  };
};

type MicDebug = {
  echoCancellation?: boolean;
  noiseSuppression?: boolean;
  autoGainControl?: boolean;
  sampleRate?: number;
  channelCount?: number;
};

type PlaybackCursor = {
  turnId: string;
  responseId: string;
  lastPlayedSequence: number;
  playedAudioMs: number;
  startedAt: number;
};

const BARGE_IN_ECHO_SUPPRESSION_MS = 350;
const BARGE_IN_BROWSER_RMS_THRESHOLD = 0.025;
const BARGE_IN_BROWSER_STRONG_RMS_THRESHOLD = 0.055;
const BARGE_IN_BROWSER_CONFIRMATION_MS = 90;

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
  const callIdRef = useRef("");
  const stream = useRef<MediaStream | null>(null);
  const audioContext = useRef<AudioContext | null>(null);
  const processor = useRef<ScriptProcessorNode | null>(null);
  const processorSink = useRef<GainNode | null>(null);
  const playbackAt = useRef(0);
  const playbackSources = useRef<Set<AudioBufferSourceNode>>(new Set());
  const playbackGain = useRef<GainNode | null>(null);
  const activePlayback = useRef<PlaybackCursor | null>(null);
  const cancelledResponses = useRef<Set<string>>(new Set());
  const lastCandidateByResponse = useRef<Map<string, number>>(new Map());
  const candidateSpeechStart = useRef<{responseId: string; startedAt: number} | null>(null);
  const speculativelyDuckedResponse = useRef<string | null>(null);

  const setPlaybackVolume = useCallback((volume: number) => {
    const gain = playbackGain.current;
    const context = audioContext.current;
    if (!gain || !context) return;
    gain.gain.cancelScheduledValues(context.currentTime);
    gain.gain.setTargetAtTime(volume, context.currentTime, 0.03);
  }, []);

  const restoreSpeculativePlayback = useCallback((responseId?: string) => {
    if (responseId && speculativelyDuckedResponse.current !== responseId) return;
    speculativelyDuckedResponse.current = null;
    candidateSpeechStart.current = null;
    setPlaybackVolume(1);
  }, [setPlaybackVolume]);

  const stopPlayback = useCallback(() => {
    restoreSpeculativePlayback();
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
  }, [restoreSpeculativePlayback]);

  const sendPlaybackProgress = useCallback(() => {
    const socket = websocket.current;
    const cursor = activePlayback.current;
    if (!socket || socket.readyState !== WebSocket.OPEN || !cursor) return;
    const context = audioContext.current;
    const elapsedMs = context
      ? Math.max(0, (context.currentTime - cursor.startedAt) * 1000)
      : cursor.playedAudioMs;
    const playedAudioMs = Math.max(cursor.playedAudioMs, elapsedMs);
    cursor.playedAudioMs = playedAudioMs;
    socket.send(JSON.stringify({
      type: "playback.progress",
      call_id: callIdRef.current,
      turn_id: cursor.turnId,
      response_id: cursor.responseId,
      last_played_sequence: cursor.lastPlayedSequence,
      played_audio_ms: playedAudioMs,
    }));
  }, []);

  const sendPlaybackDone = useCallback((responseId: string) => {
    const socket = websocket.current;
    const cursor = activePlayback.current;
    if (!socket || socket.readyState !== WebSocket.OPEN || !cursor) return;
    if (cursor.responseId !== responseId) return;
    sendPlaybackProgress();
    socket.send(JSON.stringify({
      type: "playback.done",
      call_id: callIdRef.current,
      turn_id: cursor.turnId,
      response_id: cursor.responseId,
      last_played_sequence: cursor.lastPlayedSequence,
      played_audio_ms: cursor.playedAudioMs,
    }));
  }, [sendPlaybackProgress]);

  const stopCapture = useCallback(() => {
    if (processor.current) {
      processor.current.onaudioprocess = null;
      processor.current.disconnect();
      processor.current = null;
    }
    if (processorSink.current) {
      processorSink.current.disconnect();
      processorSink.current = null;
    }
    stream.current?.getTracks().forEach((track) => track.stop());
    stream.current = null;
  }, []);

  const playPcm = useCallback((
    base64: string,
    sampleRate: number,
    turnId: string,
    responseId: string,
    sequence: number,
  ) => {
    if (cancelledResponses.current.has(responseId)) return;
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
    if (!playbackGain.current || playbackGain.current.context !== context) {
      playbackGain.current?.disconnect();
      const gain = context.createGain();
      gain.gain.value = speculativelyDuckedResponse.current === responseId ? 0.18 : 1;
      gain.connect(context.destination);
      playbackGain.current = gain;
    }
    source.connect(playbackGain.current);
    const startAt = Math.max(context.currentTime + 0.03, playbackAt.current);
    const cursor = activePlayback.current;
    if (!cursor || cursor.responseId !== responseId) {
      restoreSpeculativePlayback();
      activePlayback.current = {
        turnId,
        responseId,
        lastPlayedSequence: 0,
        playedAudioMs: 0,
        startedAt: startAt,
      };
    }
    source.start(startAt);
    playbackSources.current.add(source);
    source.onended = () => {
      playbackSources.current.delete(source);
      source.disconnect();
      const active = activePlayback.current;
      if (active?.responseId === responseId) {
        active.lastPlayedSequence = Math.max(active.lastPlayedSequence, sequence);
        active.playedAudioMs += buffer.duration * 1000;
        sendPlaybackProgress();
        if (playbackSources.current.size === 0) sendPlaybackDone(responseId);
      }
    };
    playbackAt.current = startAt + buffer.duration;
  }, [restoreSpeculativePlayback, sendPlaybackDone, sendPlaybackProgress]);

  const maybeSendBargeInCandidate = useCallback((samples: Float32Array) => {
    const socket = websocket.current;
    const cursor = activePlayback.current;
    if (!socket || socket.readyState !== WebSocket.OPEN || !cursor) return;
    if (playbackSources.current.size === 0) return;
    const context = audioContext.current;
    if (context) {
      const playbackElapsedMs = (context.currentTime - cursor.startedAt) * 1000;
      if (playbackElapsedMs < BARGE_IN_ECHO_SUPPRESSION_MS) return;
    }
    let sum = 0;
    for (let index = 0; index < samples.length; index += 1) sum += samples[index] * samples[index];
    const rms = Math.sqrt(sum / Math.max(samples.length, 1));
    const now = performance.now();
    if (rms < BARGE_IN_BROWSER_RMS_THRESHOLD) {
      if (candidateSpeechStart.current?.responseId === cursor.responseId) {
        candidateSpeechStart.current = null;
      }
      return;
    }
    if (candidateSpeechStart.current?.responseId !== cursor.responseId) {
      candidateSpeechStart.current = {responseId: cursor.responseId, startedAt: now};
      return;
    }
    if (now - candidateSpeechStart.current.startedAt < BARGE_IN_BROWSER_CONFIRMATION_MS) return;
    const lastSent = lastCandidateByResponse.current.get(cursor.responseId) ?? 0;
    if (now - lastSent < 1000) return;
    lastCandidateByResponse.current.set(cursor.responseId, now);
    const playedAudioMs = context
      ? Math.max(cursor.playedAudioMs, (context.currentTime - cursor.startedAt) * 1000)
      : cursor.playedAudioMs;
    cursor.playedAudioMs = Math.max(0, playedAudioMs);
    if (rms >= BARGE_IN_BROWSER_STRONG_RMS_THRESHOLD) {
      stopPlayback();
    } else {
      speculativelyDuckedResponse.current = cursor.responseId;
      setPlaybackVolume(0.18);
    }
    socket.send(JSON.stringify({
      type: "client.barge_in_candidate",
      barge_in_id: crypto.randomUUID(),
      call_id: callIdRef.current,
      turn_id: cursor.turnId,
      response_id: cursor.responseId,
      detected_at_monotonic_ms: now,
      last_played_sequence: cursor.lastPlayedSequence,
      played_audio_ms: cursor.playedAudioMs,
    }));
  }, [setPlaybackVolume, stopPlayback]);

  async function startCall() {
    stopPlayback();
    stopCapture();
    websocket.current?.close();
    websocket.current = null;
    const id = crypto.randomUUID();
    setCallId(id);
    callIdRef.current = id;
    setEvents([]);
    setPartialTranscript("");
    setTranscript("");
    setResponse("");
    setIgnoredNoiseTurns(0);
    setMicDebug({});
    setPipeline({stage: "transport", corked: false, queue_depths: {}});
    cancelledResponses.current.clear();
    activePlayback.current = null;
    lastCandidateByResponse.current.clear();
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
      const mutedSink = context.createGain();
      mutedSink.gain.value = 0;
      processor.current = node;
      processorSink.current = mutedSink;
      socket.send(JSON.stringify({type: "audio.config", sample_rate: context.sampleRate, channels: 1}));
      node.onaudioprocess = (event) => {
        if (socket.readyState === WebSocket.OPEN) {
          const samples = event.inputBuffer.getChannelData(0);
          maybeSendBargeInCandidate(samples);
          socket.send(floatToInt16(samples));
        }
      };
      source.connect(node);
      node.connect(mutedSink);
      mutedSink.connect(context.destination);
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
      if (data.type === "llm.token") {
        if (!data.response_id || !cancelledResponses.current.has(data.response_id)) {
          setResponse((current) => current + data.text);
        }
      }
      if (data.type === "audio.chunk") {
        playPcm(
          data.audio,
          data.sample_rate,
          data.turn_id,
          data.response_id,
          data.sequence,
        );
      }
      if (data.type === "pipeline.response_cancelled") {
        if (data.response_id) cancelledResponses.current.add(data.response_id);
        stopPlayback();
        activePlayback.current = null;
      }
      if (data.type === "pipeline.event") {
        setEvents((current) => [...current.slice(-199), data.event]);
        setPipeline(data.state);
        if (data.event?.event_type === "user.barge_in_rejected") {
          restoreSpeculativePlayback(data.event.payload?.response_id);
        }
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
    cancelledResponses.current.clear();
    activePlayback.current = null;
    lastCandidateByResponse.current.clear();
    void audioContext.current?.close();
    audioContext.current = null;
    setConnected(false);
    setRecording(false);
    setCallId("");
    callIdRef.current = "";
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
      <div className="grid three">
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
        <div className="card">
          <h3>Barge-in</h3>
          <div className="mono muted">
            state: {pipeline.barge_in?.state ?? "IDLE"}<br />
            response: {pipeline.barge_in?.active_response_id ?? pipeline.active_response_id ?? "none"}<br />
            candidate: {pipeline.barge_in?.candidate_id ?? "none"}<br />
            semantic: {pipeline.barge_in?.last_semantic ?? "none"}<br />
            played: {Math.round(pipeline.barge_in?.playback?.played_audio_ms ?? 0)} ms
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
