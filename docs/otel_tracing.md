# OpenTelemetry Tracing And Metrics

Observability must follow both the synchronous session runtime and asynchronous work.
VoiceMesh exports locally through the OpenTelemetry Collector to Jaeger, Prometheus, and
Grafana. In the current architecture, Prometheus and Grafana are for live operational
health, SLOs, and incident debugging. They are not the long-range analytical store.

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

The live coroutine chain continues the current span context through the session worker.
Kafka producers inject W3C `traceparent` and `tracestate` headers, and consumers extract
those headers before creating `kafka.consume` and downstream projection spans. This makes
a normal call trace cross:

`voicemesh-api -> Kafka publish -> voicemesh-event-worker -> Postgres projection`

Temporal receives explicit safe trace context in lifecycle signal payloads, and
activities extract that context before creating `temporal.activity.*` spans. This is
enough for the local lab to correlate important durable lifecycle work with the call
that caused it.

Remaining production hardening:

- use Temporal interceptors for workflow/activity propagation instead of explicit
  payload fields;
- propagate context through customer webhook/tool calls;
- decide where async consumers should create child spans versus linked spans; and
- avoid placing raw audio, full transcripts, prompts, or secrets in span attributes.

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

- queued speak-ahead milliseconds and queued audio milliseconds;
- cork and uncork count plus duration;
- barge-in cancellation latency;
- stale tokens and audio chunks dropped;
- provider timeout and error rate by provider/model;
- Kafka produce failures and consumer lag;
- Postgres pool wait, transaction latency, retry count, and failure rate;
- Temporal workflow/activity latency, retries, and stuck executions;
- webhook delivery latency, retry count, and terminal failure; and
- active calls, admission rejects, and per-tenant concurrency.

The current POC exposes stage latency, LLM first-token latency, TTS first-audio latency,
weighted queue depth, queue item count, hard-limit events, stale-chunk drops,
backpressure transitions/duration, duplicates, provider failures, Kafka consumer lag,
event-worker batch size/duration, Postgres projection latency/errors, Temporal
workflow/activity counters, billing readiness observations, webhook delivery attempts,
DB write failures, and active calls. It does not yet measure true end-of-speech to
first audible client playback, browser stop-playback latency, or Postgres pool wait.

## Prometheus And Grafana Scope

Prometheus is the primary datasource for current Grafana dashboards. The dashboards are
provisioned from `infra/grafana/dashboards` and are organized around operational
questions:

- `VoiceMesh Overview`: active calls, call lifecycle rate, provider failures, latency,
  weighted queue depth, corking, Kafka lag, Postgres projection health, Temporal
  failures, and webhook failures.
- `VoiceMesh Live Pipeline Latency`: STT, LLM, TTS, and transport-lag proxy panels for
  the hot path.
- `VoiceMesh Backpressure & Corking`: queued speak-ahead/audio milliseconds, queue
  items, hard-limit events, stale drops, cork/uncork counts, cork duration, and the
  stages causing the most corking.
- `VoiceMesh Provider Reliability`: provider failure/fallback and provider latency
  panels.
- `VoiceMesh Kafka & Postgres Projection Health`: Kafka consumer lag, batch processing,
  projection throughput, projection latency, duplicates, dead-letter activity, and DB
  failures.
- `VoiceMesh Temporal Outer Loop`: workflow starts/completions/failures and activity
  duration for durable outer-loop work only.
- `VoiceMesh Billing & Webhook Operational Health`: billing finalization health,
  waiting states, adjustments, webhook attempts, and webhook latency.

Prometheus labels must remain stable and low-cardinality. Do not use `call_id`,
`tenant_id`, transcript text, raw error strings, or free-form cork reasons as labels.
Use labels such as `stage`, `provider`, `model`, `topic`, `consumer_group`,
`transition`, and `reason_code`. Put high-cardinality identifiers in traces, logs,
Kafka events, and Postgres projections instead.

## Future ClickHouse Analytics

VoiceMesh does not have ClickHouse yet. When added, ClickHouse should back analytical
dashboards that are not appropriate for Prometheus:

- call volume by tenant or assistant over long windows;
- cost by provider, model, tenant, or customer;
- historical usage trends across STT, LLM, and TTS providers;
- full call event timelines and arbitrary event exploration;
- transcript and tool analytics; and
- historical failure analysis over months of events.

Do not add those warehouse-style panels to the current Prometheus dashboards.

## Local Tooling

Open Jaeger at `http://localhost:16686`, select `voicemesh-api`, and search for
operation `voice.call`. The call detail page also exposes the trace ID emitted on
pipeline events; paste that ID directly into Jaeger when a trace is hard to find.

For a useful call trace, expect to see:

- `voice.call` as the parent span for a browser/WebSocket call;
- `websocket.receive` and `websocket.send` spans with message type and audio-byte
  attributes;
- `pipeline.vad`, `pipeline.stt`, `pipeline.llm`, `pipeline.tts`, and
  `pipeline.backpressure` spans;
- `provider.openai.stt.connect`, `provider.openai.stt.commit`,
  `provider.openai.llm.responses_stream`, and `provider.openai.tts.speech_stream`
  spans with provider/model/endpoint and first-token or first-audio timing where
  applicable;
- `kafka.publish` spans from the API service;
- `kafka.consume` and `postgres.project_event` spans from `voicemesh-event-worker`; and
- `temporal.activity.*` spans from `voicemesh-temporal-worker` for lifecycle work.

FastAPI `/metrics` requests are intentionally excluded from tracing so Prometheus
scrapes do not bury real call traces. Jaeger uses a local Badger volume
(`jaeger-data`) instead of in-memory storage, so traces survive normal container
restarts. Removing the Compose volume still deletes local trace history.

Grafana at `http://localhost:3001` contains the local VoiceMesh dashboard folder. These
Compose services demonstrate instrumentation flow; they are not a production
observability deployment or retention design.
