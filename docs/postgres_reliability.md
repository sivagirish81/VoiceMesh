# Postgres Reliability

## Idempotency

`idempotency_keys.idempotency_key` and `call_events.idempotency_key` are unique. The
repository inserts the key before applying the event effect in the same transaction.
A conflict means the delivery is a duplicate and no second transition is applied.

## Transactional Outbox

Critical DB-to-Kafka events insert their outbox row in the same transaction as the
idempotency key and call event. The publisher selects unpublished rows with
`FOR UPDATE SKIP LOCKED`, sends them to Kafka, and updates `published`, `attempts`, and
`last_error`.

Kafka's idempotent producer reduces duplicate production from retry, while consumer-side
idempotency remains necessary because the system is intentionally at least once.

## Pool Limits and Retry

The API defaults to a pool of 2–10 connections with a five-second command timeout.
Transient write errors receive three bounded attempts with exponential delay. The
retry budget is short because unbounded DB waits would damage live-call latency.

## Safe Degradation

Metric and non-critical lifecycle writes can fail without immediately terminating audio
processing. Kafka publication occurs independently for live events. Critical effects
that require DB truth can raise after the retry budget.

## DB-Down Demo Limitations

After Postgres resumes, the pool and outbox loop accept new work. Outbox rows committed
before the outage are eventually published. A non-critical write that failed all
attempts during the outage is not reconstructed automatically. A production system
would add a bounded local/Kafka recovery journal and reconciliation consumer.

