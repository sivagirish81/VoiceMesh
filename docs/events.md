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
- `tool.call_requested`, `tool.call_completed`
- `webhook.delivery_requested`, `webhook.delivered`
- `billing.usage_recorded`

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

The current POC publishes `llm.token`, `tts.audio_chunk`, and
`transport.audio_sent` metadata to make the lab timeline visible. That is current
instrumentation, not the recommended production default.

## Partitioning And Ordering

Partition call events primarily by `call_id`. This preserves order for one call while
allowing calls from the same tenant to scale across partitions. `tenant_id` remains
metadata for authorization, quotas, billing, retention, and observability; partitioning
only by tenant can create hot partitions for large customers.

Ordering is guaranteed only within a partition. Consumers must tolerate retries,
duplicates, delayed events, and independent event types arriving from different
producers.

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

The current POC directly publishes an event and, for events marked critical, also
inserts the same event into the outbox. Unique database keys reduce repeated persistence
effects, but downstream Kafka consumers could still see two deliveries. Production
work should choose a single route per event.

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

The event should retain a correlation `trace_id`, but asynchronous trace continuation
should use W3C `traceparent` and `tracestate` Kafka headers. Consumers create linked or
continued spans according to the processing model. Serialized trace IDs alone are
useful for search but do not provide complete context propagation.
