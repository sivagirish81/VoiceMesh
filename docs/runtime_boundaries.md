# Runtime Boundaries

This document defines which work belongs in the live session runtime and which work
belongs in asynchronous infrastructure.

## The Hot Path

The hot path is the latency-sensitive chain that turns incoming speech into audible
agent output:

`Transport Gateway → Session Worker → VAD → streaming STT → streaming LLM → phrase buffer → streaming TTS → Transport`

It should remain in memory inside one per-call session worker. Kafka, Postgres, and
Temporal may observe or react to the call, but they are not stage-to-stage transports.

The session worker owns the active transport, provider streams, call and turn identity,
weighted bounded queues, playback state, cancellation tokens, barge-in state, and
transient metrics. A worker crash ends the active media connection unless the transport
layer can reconnect and rehydrate it; Temporal history cannot reconstruct an ephemeral
provider socket or browser audio buffer.

## Current POC Runtime

`StreamModule` is the current session worker. It:

- receives real PCM from `BrowserWebSocketTransport`;
- runs pluggable VAD, defaulting to WebRTC VAD over a normalized 16 kHz PCM copy;
- smooths VAD output through `QUIET`, `STARTING`, `SPEAKING`, and `STOPPING` endpointing;
- suppresses short, sparse, or empty-transcript noise turns before they reach the LLM;
- keeps an OpenAI Realtime transcription WebSocket open for the call;
- resamples browser PCM to 24 kHz, appends frames as speech arrives, displays transcript
  deltas, and commits the provider buffer at the VAD/end-turn boundary;
- streams OpenAI LLM deltas into a weighted text queue;
- groups text at punctuation or a character threshold;
- streams OpenAI TTS PCM into a weighted audio-duration queue; and
- sends audio to the browser over WebSocket.

This is a real streaming vertical slice. The runtime now fences queued text/audio with
`turn_id` and `response_id`, drops stale chunks, and can flush queues on cancellation.
Full-duplex browser playback interruption and provider-native cancellation are still
future production hardening.

## Streaming STT Boundary

The implemented `StreamingSTTSession` maintains a provider connection for the active
call. Audio frames flow continuously to the adapter, which provides:

- `append_audio(audio_chunk, sample_rate)`
- a partial-transcript callback
- `commit()` returning a final transcript and provider item ID
- `close()`

The LLM should normally receive only stable final user turns. Partials are useful for
captions, early intent hints, interruption detection, or sampled diagnostics, but
unstable text should not routinely trigger irreversible tools or agent speech.

The final transcript has two independent destinations:

1. direct in-memory handoff from the session worker to the LLM; and
2. asynchronous `stt.final_transcript` publication to Kafka.

The current publisher is asynchronous with respect to Postgres but is still awaited by
the session coroutine. A production worker should place the second destination behind
a bounded event publisher so broker latency cannot delay the first.

## Turn And Response Fences

Each call has monotonically advancing turns. An agent response also receives a unique
`response_id`. Every queued token, phrase, audio chunk, provider callback, and playback
acknowledgement is tagged with:

```text
tenant_id
assistant_id
call_id
turn_id
response_id
sequence
```

The worker sends an item only if its fence matches the active call state. A late TTS
chunk from response A cannot play after response B starts, even if provider cancellation
was delayed or unsupported.

Sequence numbers are scoped explicitly. A per-call event sequence preserves timeline
ordering; provider-local or media sequences can detect gaps and reordering without
pretending to provide global ordering.

## Barge-In

When user speech begins while the agent is speaking:

1. VAD marks a new user interruption.
2. The transport stops playback for the active `response_id`.
3. The TTS request is cancelled when supported.
4. The LLM request is cancelled, or its remaining output is ignored.
5. Text, phrase, and audio queues for the old response are flushed or discarded.
6. Stale provider callbacks are rejected by the response fence.
7. The next `turn_id` becomes active and incoming speech continues to STT.

The system should measure cancellation-to-silence latency and stale chunks dropped.
Completing obsolete audio is not a correctness goal. Durable finalized events should
survive once committed; live buffers may be thrown away when freshness requires it.

## Backpressure

Backpressure operates locally:

- `llm_to_tts` is bounded by estimated queued speak-ahead milliseconds;
- `tts_to_transport` is bounded by queued playable audio milliseconds;
- high watermarks pause upstream work before the queue is full;
- low watermarks resume production;
- hard limits cancel or flush stale response output according to policy;
- queue ownership is turn-scoped;
- cancellation releases waiters and drains obsolete work; and
- prolonged pressure can emit a coarse Kafka event or fail the turn.

Temporal is not part of routine cork/uncork operation. It may become relevant only when
pressure causes a lifecycle-level result, such as a failed call, deferred post-call
work, or a provider-routing action that must survive process loss.

## Transport Boundary

The browser WebSocket is the POC `TransportProvider`. It proves real microphone capture
and playback locally. A production system would normally place a media gateway in front
of session workers to terminate WebRTC, SIP/RTP, or telephony protocols, normalize
codecs, and route a call consistently to one worker.

Transport adapters should normalize receive frames, send frames, playback position,
stop-playback/cancel, disconnect reason, and network/send lag. The session worker should
not contain protocol-specific SIP or WebRTC logic.
