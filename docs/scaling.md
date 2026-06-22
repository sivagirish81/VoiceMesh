# Scaling To 1M-5M Calls

VoiceMesh scales by separating the latency-sensitive call runtime from durable data,
workflow, and analytics systems. The target is not one giant service. It is a set of
small, horizontally scalable planes with clear ownership:

- media gateways terminate WebRTC, SIP, or telephony traffic;
- session workers own active calls and provider streams;
- Kafka carries coarse durable events;
- Postgres stores transactional state and billing ledgers;
- Temporal runs durable workflows;
- ClickHouse stores historical analytics and billing-facing projections.

This document describes how to evolve the architecture toward 1M to 5M completed calls
over a production deployment window. Actual concurrency depends on call duration, region
mix, provider limits, codecs, model choice, and customer traffic shape.

## Capacity Model

Plan from concurrency, not only completed-call volume:

```text
average_concurrent_calls = calls_per_day * average_call_seconds / 86_400
peak_concurrent_calls = average_concurrent_calls * peak_multiplier
```

Example: 1M calls/day at 5 minutes average duration is about 3,472 average concurrent
calls before peak multiplier. With a 3x busy-hour multiplier, the fleet must handle
roughly 10,400 concurrent calls. At 5M calls/day, the same assumptions imply roughly
52,000 peak concurrent calls.

Each active call consumes:

- one transport session;
- one session-worker slot;
- active or idle STT, LLM, and TTS provider streams;
- bounded text/audio queues;
- Kafka producer capacity for coarse events;
- observability cardinality budget;
- tenant and provider quota.

## Deployment Plan

### Phase 1: Hardened Regional Deployment

Use one production region with replicated services:

- managed Kubernetes or equivalent compute for gateways, API, session workers, event
  workers, Temporal workers, and dashboard/API services;
- managed Kafka for durable event topics and schema governance;
- managed Postgres with read replicas, point-in-time recovery, and separate app,
  worker, and analytics roles;
- Temporal Cloud or a production Temporal cluster for durable workflows;
- managed Redis or an equivalent cache for tenant config and admission decisions;
- ClickHouse Cloud as an optional analytics store.

The hot path remains:

```text
Transport gateway -> Session worker -> STT -> LLM -> TTS -> Transport gateway
```

Kafka, Postgres, Temporal, and ClickHouse sit beside this path. They must not become
required hops between provider stages.

### Phase 2: Multi-Region Active/Active

Route calls to the closest healthy region that has provider capacity and satisfies the
tenant's data policy. Keep one active call pinned to one region and one session worker.

Recommended regional shape:

- regional media gateways for network proximity;
- regional session-worker pools sized by concurrent calls;
- regional Kafka clusters or managed Kafka environments;
- regional Temporal namespaces or task queues for durable work placement;
- regional Postgres primaries for tenants pinned to that region, or a carefully chosen
  multi-region Postgres design for shared tenants;
- regional ClickHouse ingestion with global dashboards over replicated or federated
  analytics data.

Cross-region failover should prefer new-call admission failover. Live-call migration is
usually not worth the complexity because browser sockets, provider streams, and queued
audio are local in-memory state.

### Phase 3: Large Tenant And Enterprise Isolation

For very large tenants, isolate noisy or sensitive workloads:

- dedicated worker pools;
- dedicated provider projects or credentials;
- dedicated Kafka quotas, topics, or clusters;
- dedicated Temporal namespaces or task queues;
- separate encryption keys and retention policies;
- optional dedicated Postgres schemas, databases, or clusters;
- tenant-specific ClickHouse databases or row policies.

`tenant_id` is metadata and routing context, not a security boundary by itself.
Authorize every API, event consumer, workflow action, webhook, and analytics query.

## Call Placement

Admission control should happen before a call receives a session-worker slot. Placement
inputs include:

- tenant and assistant concurrency limits;
- available worker CPU, memory, queue depth, and open provider streams;
- regional latency to caller and providers;
- provider quota and rate-limit headroom;
- codec/transcoding cost;
- failure-domain spread;
- tenant data-residency policy.

Once admitted, the call receives a stable `call_id` and is pinned to a session worker.
The gateway routes all media and control messages for that call to the same worker until
the call ends.

## Kafka At Scale

Use managed Kafka when operational focus belongs on the voice product rather than
broker maintenance. Confluent Cloud, AWS MSK, Aiven, Redpanda Cloud, or a hardened
self-managed cluster can fit depending on cloud, compliance, and cost constraints.

Topic design:

- partition call, pipeline, provider, and usage topics primarily by `call_id`;
- keep `tenant_id`, `assistant_id`, `turn_id`, and `response_id` in the event envelope;
- keep raw audio, every token, every TTS chunk, and every VAD frame off the durable bus;
- use a schema registry with compatibility checks;
- set retention by replay need, not habit;
- create separate consumer groups for Postgres projection, Temporal starters,
  ClickHouse ingestion, evaluations, billing exports, and debug tooling.

For 1M to 5M calls, partition counts should be set from measured producer throughput,
consumer lag, event size, retention, and replay requirements. Increase partitions
deliberately because key ordering and consumer parallelism are architectural contracts.

## Postgres And CDC

Postgres is the transactional source of truth for tenants, assistants, calls, usage,
billing ledgers, idempotency keys, outbox rows, webhook state, and workflow-facing state.

At scale:

- separate OLTP traffic from analytics reads;
- use connection pooling and per-service database roles;
- keep writes idempotent with deterministic keys;
- partition or archive high-volume tables by time and tenant;
- keep immutable ledger records append-only;
- use read replicas for admin APIs and support tooling;
- stream billing and operational tables to ClickHouse with CDC.

CDC is the right path when analytics needs the exact committed state from Postgres.
PeerDB or ClickHouse ClickPipes for Postgres can replicate committed Postgres changes
into ClickHouse Cloud. Kafka consumers are better for event-time analytics. Many
production deployments use both:

- Kafka -> ClickHouse for coarse call events and latency timelines;
- Postgres CDC -> ClickHouse for billing ledger, manifests, adjustments, tenant
  dimensions, and invoice-facing analytics.

## Temporal At Scale

Temporal runs durable workflows that can outlive a call:

- `CallCompletionWorkflow`;
- `BillingFinalizationWorkflow`;
- `BillingAdjustmentWorkflow`;
- `WebhookDeliveryWorkflow`;
- `DurableActionWorkflow`;
- summary, evaluation, transcript, and recording finalization workflows.

Temporal Cloud is the default managed option when the team wants hosted availability,
namespaces, metrics, and operational support. A self-managed Temporal cluster can work
when infrastructure control or deployment constraints require it.

Scaling rules:

- use deterministic workflow IDs by business entity;
- split task queues by workflow family and region;
- run Temporal workers separately from session workers;
- keep high-volume media events out of workflow history;
- prefer signals as wake-up hints, with Postgres as source of truth for usage readiness;
- enforce activity idempotency for Postgres, provider, webhook, and external API calls;
- monitor schedule-to-start latency, activity failures, workflow backlog, and namespace
  limits.

## ClickHouse Analytics

ClickHouse Cloud is an optional but natural analytics target once call volume reaches
millions of events per day. It should store coarse, queryable facts:

- call lifecycle and outcome;
- STT/LLM/TTS latency milestones;
- provider, model, region, and error dimensions;
- backpressure and stale-output incidents;
- barge-in and noise-handling outcomes;
- usage and billing aggregates;
- webhook and workflow completion events.

Use low-cardinality dimensions carefully and avoid storing raw audio, full transcripts,
every token, or every frame. Keep Prometheus for live alerts, Jaeger or tracing storage
for single-call debugging, Postgres for transactional truth, and ClickHouse for
historical cross-call analysis.

## Quotas And Fairness

Large-scale voice systems need admission control before degradation reaches callers:

- per-tenant concurrent calls;
- per-assistant concurrent calls;
- provider-specific stream and request quotas;
- token and audio-minute budgets;
- webhook and tool execution budgets;
- bounded in-memory queues per call;
- global circuit breakers for provider incidents;
- fair scheduling across tenants and worker pools.

When limits are hit, fail admission cleanly, route to a fallback provider, degrade to a
known safe mode, or return a tenant-visible capacity error. Do not let queues grow
unbounded inside the live media path.

## Observability And Operations

At 1M to 5M calls, observability must be sampled, aggregated, and cardinality-aware:

- trace full calls for sampled traffic, failures, and tenant-triggered diagnostics;
- keep `call_id` out of Prometheus labels;
- use Kafka and ClickHouse for per-call drilldown;
- track end-of-speech to first audible audio as the primary user-facing latency SLI;
- track provider quota, first-token latency, first-audio latency, queue depth, cork
  duration, stale chunks, Kafka lag, Postgres pool wait, Temporal backlog, and webhook
  retry age;
- create runbooks for provider outage, Kafka lag, Postgres failover, Temporal backlog,
  ClickHouse ingestion delay, and regional call-admission failure.

## Practical Evolution

The clean evolution path is:

1. Make the local vertical slice correct and observable.
2. Move Kafka, Postgres, Temporal, and ClickHouse to managed production services.
3. Add schema registry, tenant-aware envelopes, quotas, and admission control.
4. Split gateways, session workers, event workers, Temporal workers, and analytics
   consumers into independently scaled deployments.
5. Add CDC from Postgres to ClickHouse for billing and ledger analytics.
6. Add multi-region call placement and regional failure isolation.
7. Create dedicated pools and data planes for the largest tenants.

The core rule stays the same from one call to five million: the active conversation is
owned by the session worker, and durable systems receive coarse facts and business
workflows around it.
