# Provider Adapters

Provider adapters keep the session runtime independent of OpenAI, Deepgram, Cartesia,
local models, and transport vendors. The core pipeline should consume normalized
events, media, and cancellation semantics rather than provider-specific response
objects.

## Target Contracts

The production-oriented adapter set is:

- `STTProvider`
- `LLMProvider`
- `TTSProvider`
- `TransportProvider`
- optional `ToolExecutor`

An STT adapter accepts audio frames continuously and emits speech boundaries plus
partial and final transcripts. An LLM adapter emits token deltas, tool requests, usage,
and completion. A TTS adapter accepts phrase input and emits timed audio chunks. A
transport adapter normalizes receive/send media, playback stop, acknowledgements,
disconnects, and send lag.

All adapters should support a common lifecycle:

- open or start a stream;
- send input;
- receive normalized events;
- close input;
- cancel by `turn_id` and `response_id`;
- expose first-result and total latency;
- map timeout, quota, authentication, and transient errors; and
- retain provider-specific metadata in an optional namespaced field.

## Normalized Identity

Provider input and output carries `tenant_id`, `assistant_id`, `call_id`, `turn_id`,
`response_id`, and sequence information. The session worker uses these fences to reject
late output after cancellation or barge-in.

Adapters should not decide whether a stale chunk is playable. They report the identity;
the session runtime owns the active-turn decision.

## Current OpenAI Adapters

The POC registry is the construction boundary and fails fast when OpenAI is selected
without `OPENAI_API_KEY`. It currently implements:

- `OpenAISTTProvider.transcribe`, which submits one finalized WAV to
  `gpt-4o-transcribe`;
- `OpenAILLMProvider.stream_generate`, which streams text deltas from
  `gpt-4.1-mini`; and
- `OpenAITTSProvider.synthesize`, which streams PCM from `gpt-4o-mini-tts`.

`StreamModule` receives interface instances and does not import concrete OpenAI classes.
That is a useful boundary, but the current STT interface is one-shot, transport is typed
directly as `BrowserWebSocketTransport`, and cancellation is not part of the contracts.

## Production Direction

Evolve the interfaces without changing the session algorithm:

- replace one-shot `transcribe(bytes)` with a streaming STT session;
- add provider-neutral partial/final transcript events;
- add `response_id` and cancellation to LLM and TTS;
- normalize tool-call requests and usage;
- expose time to first token and first audio byte from adapters;
- move audio format conversion into adapters or a media normalization layer; and
- depend on `TransportProvider`, not the browser implementation.

Deepgram or another streaming STT provider, OpenAI or another LLM, Cartesia or another
TTS provider, and SIP/WebRTC transports should all sit behind these contracts.

## Local Providers

Local Whisper, Ollama, and Piper can be added as adapters. Model lifecycle, batching,
GPU/CPU scheduling, process supervision, audio formats, and provider-specific timeouts
remain inside those implementations. The session worker still sees normalized events.

Local does not mean fake. A local adapter must invoke the real engine and report its
actual latency and failures.

## Failover

Live failover has a strict latency budget. The session worker or a low-latency routing
service should select a preconfigured fallback and advance the response fence. Temporal
may coordinate durable remediation or post-call actions, but waiting on a workflow
round trip is usually inappropriate inside a live turn.

Provider policy belongs in versioned tenant configuration with health, quota, region,
cost, and capability constraints. A fallback must be semantically compatible with the
required streaming and cancellation contract.
