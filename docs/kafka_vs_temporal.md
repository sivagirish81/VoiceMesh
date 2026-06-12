# Kafka vs. Temporal

Kafka and Temporal are both durable, but they solve different problems.

## Kafka

Kafka is the high-throughput event stream for call, pipeline, and provider observations.
It supports independent consumers, replay, partition ordering by call key, analytics,
and operational inspection. Token events and fine-grained stage events belong here.

## Temporal

Temporal is the durable call lifecycle. It owns explicit states, timers, retries,
provider-degradation decisions, recovery work, and final outcomes. It reconstructs
workflow state by replaying deterministic history after worker restart.

## Deliberate Boundary

Raw audio chunks never enter Temporal. They are high-volume, latency-sensitive, and
already belong to the media transport. Individual LLM tokens also stay out of Temporal.

Temporal receives only high-level signals:

- call started,
- pipeline corked or uncorked,
- provider failed,
- call completed, and
- call failed.

This prevents workflow history from becoming a second media/event bus while preserving
durable business-level coordination.

