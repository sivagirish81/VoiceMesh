# Event Contracts

Kafka records durable, coarse-grained facts about calls and surrounding business
processes. It is not the normal live carrier for media frames, provider tokens, or TTS
chunks.

## Recommended Envelope

```json
{
  "event_id": "uuid",
  "event_type": "stt.final_transcript",
  "event_version": 1,
  "tenant_id": "tenant_123",
  "assistant_id": "assistant_456",
  "call_id": "call_789",
  "turn_id": "turn_3",
  "response_id": null,
  "sequence": 42,
  "timestamp": "2026-06-15T20:30:00Z",
  "trace_id": "32-hex-character trace id",
  "traceparent": "00-...",
  "idempotency_key": "call_789:turn_3:stt.final_transcript:v1",
  "payload": {}
}
```

`turn_id` and `response_id` are optional for call-level events. `event_id` identifies
the logical event and remains stable across publication retries. `idempotency_key`
identifies the logical consumer effect and is also deterministic. If transport-level
delivery attempts need separate identity, add a distinct `delivery_id`.

## Coarse Event Set

Recommended production events include:

- `call.started`, `call.ended`, `call.failed`
- `user.turn.started`, `user.turn.ended`
- `stt.final_transcript`
- sampled or diagnostic `stt.partial_transcript`
- `llm.response_started`, `llm.first_token`, `llm.response_completed`
- `llm.tool_call_requested`
- `tts.first_audio_byte`, `tts.completed`, `tts.cancelled`
- `user.barged_in`
- `provider.error`, `provider.timeout`
- `tool.action.requested`, `tool.action.accepted`, `tool.action.cancel_requested`,
  `tool.action.cancelled`, `tool.action.completed`, `tool.action.failed`
- `webhook.delivery_requested`, `webhook.delivered`, `webhook.failed`
- `usage.finalization_barrier`, `billing.usage_recorded`, `billing.finalized`,
  `billing.adjustment_created`

VoiceMesh currently routes tool events to `tool-events`, webhook events to
`webhook-events`, usage events to `usage-events`, and final billing events to
`billing-events`.

Pipeline pressure events can be emitted when they are operationally meaningful, such as
a prolonged cork, a threshold breach, or a turn failure. Routine queue oscillation is
better represented as metrics.

## Events To Avoid By Default

Do not publish these continuously on the primary durable event bus:

- every 20 ms input audio frame;
- every STT partial transcript;
- every LLM token;
- every TTS audio chunk; or
- every transport write.

Those streams multiply broker traffic, storage, consumer cost, and sensitive-data
surface area. For debugging, use bounded sampling, per-call diagnostic flags, aggregate
events, metrics, or traces.

The current POC follows this rule. It sends `llm.token` and `audio.chunk` only over the
live browser WebSocket. Kafka receives LLM/TTS milestones and one aggregated
`transport.audio_sent` event per response.

## Partitioning And Ordering

Partition call and usage events primarily by `call_id`. This preserves order for one call while
allowing calls from the same tenant to scale across partitions. `tenant_id` remains
metadata for authorization, quotas, billing, retention, and observability; partitioning
only by tenant can create hot partitions for large customers.

Ordering is guaranteed only within a partition. Consumers must tolerate retries,
duplicates, delayed events, and independent event types arriving from different
producers.

Billing finalization uses `usage.finalization_barrier` on `usage-events`, keyed by
`call_id`, so the barrier is ordered behind prior usage events for that call on the same
topic partition. The UsageWriter persists the barrier's consumed Kafka
topic/partition/offset and advances `projection_watermarks` only after the DB projection
transaction succeeds.

## Direct Kafka Versus Outbox

Use one authoritative route for each logical event:

- A business event that must be atomic with a Postgres state change uses a Postgres
  transaction plus outbox row. The outbox publisher is its Kafka producer.
- A non-critical operational or media milestone that has no associated database
  transaction may publish directly to Kafka.
- Producers retain the same deterministic `event_id` and `idempotency_key` across
  retries.
- Consumers apply idempotent effects using those stable identifiers.

Avoid publishing the same logical event directly and through an outbox unless the event
contract explicitly defines deduplication. An unclear dual-write design can turn a
normal retry into a duplicate-message incident.

The current boundary is explicit:

- session lifecycle, pipeline, provider, and usage facts publish directly to Kafka;
- the event worker consumes them and applies one idempotent Postgres transaction; and
- `billing.usage_recorded`, which is atomically tied to a usage/billing DB projection,
  is written to `outbox_events` and published by the outbox worker.

The same logical billing event is not also directly published by the session worker.

## Current Topics

| Topic | Current data |
|---|---|
| `call-events` | call lifecycle and workflow state |
| `pipeline-events` | VAD, STT/LLM/TTS milestones, backpressure, aggregated transport, duplicate/DB signals |
| `provider-events` | provider failures and fallback selections |
| `usage-events` | STT duration, LLM token usage, TTS estimated usage, and finalization barriers |
| `billing-events` | outbox-published billing rollup, finalized, and adjustment events |
| `outbox-events` | reserved topic; the active outbox rows currently target their owned business topic |
| `dead-letter-events` | fallback for unmapped contracts |

The local Compose cluster is one Apache Kafka 3.9.1 KRaft broker. Each application
topic has three partitions and replication factor one, with data on the `kafka-data`
volume. Kafka UI manages inspection at `http://localhost:8081`; `kafka-init` owns
idempotent topic creation. This is useful locally but is not highly available.

## Schema Evolution

`event_type` and `event_version` form the contract identity. Producers should:

- add optional fields compatibly;
- avoid changing the meaning or type of existing fields;
- publish a new version for breaking payload changes; and
- support a measured migration window where consumers accept old and new versions.

Consumers should ignore unknown optional fields and reject unsupported major versions
to a dead-letter path with enough context for replay.

The POC uses Pydantic validation but does not run a schema registry. A production Kafka
deployment should consider a registry with Avro, Protobuf, or JSON Schema compatibility
checks in CI and at producer deployment time.

## Trace Propagation

Events retain a correlation `trace_id`, and Kafka producers inject W3C `traceparent`
and `tracestate` headers on produced records. Kafka consumers extract those headers
before creating consume/projection spans, so the local lab can show API publish,
event-worker consume, and Postgres projection work in the same Jaeger trace.

Serialized trace IDs are still useful for search and UI links, but full asynchronous
trace continuation should rely on propagated context. Production consumers may choose
continued spans or linked spans depending on whether the downstream work is causally in
line with the request or a later fanout/replay process.
