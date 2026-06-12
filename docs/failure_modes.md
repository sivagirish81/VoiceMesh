# Failure Modes

## STT Slow

The turn remains finalized while the STT provider call is bounded by
`TURN_TIMEOUT_SECONDS`. Latency is traced and persisted. A timeout emits
`pipeline.stage_timeout`; a provider exception emits `provider.failed` and signals
Temporal.

## LLM Slow

Time to first token and total generation latency remain visible. Slow generation does
not grow the downstream queue, but it increases turn latency and can trigger the
overall turn timeout.

## TTS Slow

LLM token production and TTS consumption are concurrent. Injected TTS delay grows the
token queue, crosses the high watermark, corks the pipeline, and eventually blocks the
producer without dropping critical text.

## Transport Overloaded

The TTS-to-transport audio queue is bounded. When browser/network sends slow down, TTS
audio production pauses at queue capacity. Low-watermark drainage uncorks the pipeline.

## Duplicate Events

Every persisted event has a unique idempotency key. A replay cannot insert a second
event or produce a second state transition. The system emits a new
`duplicate_event.ignored` observation for operator visibility.

## Database Unavailable

Writes retry three times with exponential backoff. Kafka and in-memory media processing
continue where the failed write is non-critical. The outbox resumes polling after the
database returns. Exhausted non-critical writes are not reconstructed automatically.

## Temporal Worker Crash

Workflow history remains in Temporal server storage. Signals and tasks wait while the
worker is absent. Restarting the worker replays deterministic history and resumes from
the durable state.

## Kafka Consumer Lag

Pipeline production remains decoupled from consumers until broker retention or disk
limits become relevant. Kafka UI exposes offsets. Production hardening would add
consumer-lag metrics, autoscaling, retention alarms, and dead-letter replay tooling.

