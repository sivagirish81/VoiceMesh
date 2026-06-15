# Postgres Reliability

Postgres is the durable system of record and query store. It is adjacent to the live
media path, not a synchronous stage in `VAD → STT → LLM → TTS`.

## Durable Data

A production deployment stores:

- tenants, organizations, assistants, and configuration versions;
- provider, voice, tool, and credential references;
- customer server URLs and webhook configuration;
- call metadata and final transcripts;
- summaries, evaluations, and recording metadata;
- tool-call and webhook-delivery state;
- billing and usage records;
- idempotency keys; and
- outbox rows for business events tied to database changes.

Configuration should be loaded into a versioned cache at call admission. Active turns
should not query Postgres before invoking each provider.

## Persistence Boundary

Final transcripts and call lifecycle events should normally reach Postgres through
Kafka consumers or outbox workers. A database outage may delay query visibility, but it
must not add a blocking database round trip between live provider stages.

The current POC awaits `create_call`, `persist_event`, `record_metric`, and
`update_call_state` from the session runtime. Retries are bounded, so many failures
degrade safely, but those waits still affect live latency. Moving these writes behind
an asynchronous writer is a production priority.

## Idempotency

At-least-once delivery requires idempotent effects. A consumer should:

1. begin a database transaction;
2. insert the deterministic idempotency key;
3. stop with success if the key already exists;
4. apply the state change; and
5. commit once.

`event_id` distinguishes deliveries; `idempotency_key` identifies the logical effect.
They should not be treated as interchangeable.

The POC enforces unique keys in `idempotency_keys` and `call_events`. A duplicate
delivery cannot insert a second persisted event.

## Transactional Outbox

Use a Postgres transaction plus outbox when a Kafka business event must be atomic with a
database update. The outbox publisher selects unpublished rows with
`FOR UPDATE SKIP LOCKED`, publishes them, and records attempts and errors.

Direct Kafka publication is appropriate for operational events that have no atomic
database state change. Each logical event should have one authoritative route.

The current POC directly publishes every event and also writes critical events to the
outbox. This can produce duplicate Kafka deliveries of the same logical event. Database
idempotency protects local writes, but every downstream consumer would still need
explicit deduplication. Production should remove this ambiguous dual path.

## Pooling, Retry, And Load Shedding

The POC pool is bounded at 2–10 connections with a five-second command timeout and
three retry attempts. Production controls should also measure pool wait time, reject or
defer non-essential writes during saturation, cap retry concurrency, and separate
interactive query pools from ingestion pools.

Retries must fit the operation's latency class. A post-call webhook row can wait longer
than a live call admission lookup. Blind retries from every session worker can amplify a
database incident.

## DB-Down Behavior

The desired behavior is:

- active audio continues from in-memory state;
- Kafka continues accepting coarse events;
- Postgres consumers pause or retry without blocking live turns;
- committed outbox rows resume after recovery; and
- reconciliation fills durable gaps where the source event still exists in Kafka.

In the current demo, Kafka and much of the in-memory path continue, but synchronous
repository calls consume their retry budget. Writes that exhaust retries are surfaced
but are not automatically reconstructed. Outbox rows committed before the outage
recover; rows never committed cannot.
