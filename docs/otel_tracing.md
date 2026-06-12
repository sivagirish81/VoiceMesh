# OpenTelemetry Tracing

The API configures an OTLP gRPC exporter to a local OpenTelemetry Collector. The
Collector sends traces to Jaeger and exposes metrics for Prometheus.

Instrumented operations include:

- FastAPI requests,
- WebSocket receive and send,
- VAD,
- STT, LLM, and TTS stages,
- Kafka publish and consume,
- Postgres event writes,
- Temporal activities, and
- backpressure transitions.

Useful span attributes include `call_id`, `turn_id`, `stage`, `provider`,
`event_type`, `queue_depth`, `latency_ms`, `corked`, `retry_count`, and
`idempotency_key`.

The browser dashboard exposes trace IDs on persisted events when a current span exists.
Open Jaeger at `http://localhost:16686`, select `voicemesh-api`, and search around the
call timestamp. Grafana at `http://localhost:3001` contains the provisioned VoiceMesh
dashboard for latency and reliability counters.

Trace context is not yet propagated through Kafka headers in this POC; the serialized
event retains a trace ID for correlation. Full W3C propagation through Kafka and
Temporal interceptors is future work.

