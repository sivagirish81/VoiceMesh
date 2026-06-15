# OpenTelemetry Tracing And Metrics

Observability must follow both the synchronous session runtime and asynchronous work.
VoiceMesh exports locally through the OpenTelemetry Collector to Jaeger, Prometheus, and
Grafana.

## Trace Coverage

Production traces should cover:

- transport gateway and session-worker admission;
- WebSocket, WebRTC, SIP, or telephony receive/send operations;
- VAD and turn finalization;
- STT, LLM, and TTS provider adapters;
- phrase buffering and bounded queues;
- cancellation, barge-in, and stale-output drops;
- Kafka produce and consume;
- Postgres writer transactions and pool waits;
- Temporal workflow starts, signals, and activities;
- tool execution; and
- customer webhook delivery.

Useful attributes include `tenant_id`, `assistant_id`, `call_id`, `turn_id`,
`response_id`, `stage`, `provider`, `event_type`, `queue_depth`, `latency_ms`,
`corked`, `retry_count`, `idempotency_key`, and provider request ID. Avoid placing raw
audio, full transcripts, prompts, or secrets in span attributes.

## Context Propagation

The live coroutine chain can continue the current span context. Kafka should propagate
W3C `traceparent` and `tracestate` headers; consumers create continued or linked spans.
Temporal should use interceptors or explicit safe context fields where appropriate.
Webhook requests should include trace correlation without exposing internal credentials.

The POC stores a `trace_id` in its serialized event but does not propagate full W3C
context through Kafka headers or Temporal. That supports search correlation, not full
distributed trace continuity.

## User-Facing Latency

The primary voice SLI is:

`end of user speech → first audible agent audio`

Break it into:

- endpointing delay;
- STT final latency;
- LLM time to first token;
- phrase-buffer delay;
- TTS time to first audio byte;
- transport send lag; and
- client playback scheduling delay.

Measure both distributions and exemplars linked to traces. A low average can hide a
poor p95 or p99 that makes conversation feel unreliable.

## Reliability Metrics

Track:

- token queue depth and audio queue depth;
- cork and uncork count plus duration;
- barge-in cancellation latency;
- stale tokens and audio chunks dropped;
- provider timeout and error rate by provider/model;
- Kafka produce failures and consumer lag;
- Postgres pool wait, transaction latency, retry count, and failure rate;
- Temporal workflow/activity latency, retries, and stuck executions;
- webhook delivery latency, retry count, and terminal failure; and
- active calls, admission rejects, and per-tenant concurrency.

The current POC exposes stage latency, queue depth, backpressure transitions/duration,
duplicates, provider failures, DB write failures, and active calls. It does not yet
measure end-of-speech to first audio, cancellation, stale drops, Kafka lag, Postgres
pool wait, or webhook delivery.

## Local Tooling

Open Jaeger at `http://localhost:16686`, select `voicemesh-api`, and search around the
call timestamp. Grafana at `http://localhost:3001` contains the local VoiceMesh
dashboard. These Compose services demonstrate instrumentation flow; they are not a
production observability deployment or retention design.
