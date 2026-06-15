# Multi-Tenant Scaling And Integrations

VoiceMesh currently runs as a single-host Docker Compose lab. This document describes
the production-oriented scaling model rather than claiming those capabilities are
already implemented.

## Tenant Configuration

A tenant or organization configures:

- assistants and prompts;
- STT, LLM, and TTS providers;
- voice and model policy;
- tools and credentials;
- server URL and webhook endpoints;
- retention, recording, and compliance policy; and
- concurrency, spend, and provider limits.

Postgres is the source of truth. A versioned cache should serve configuration to the
session worker at call admission so a live turn does not repeatedly query Postgres.
Configuration updates affect new calls by default; controlled mid-call refresh should
be explicit and versioned.

## Call Placement

Many tenants create many calls, and each active call maps to one session worker. A
transport/media gateway handles protocol termination and routes a call consistently to
that worker. Placement should consider worker capacity, geographic latency, provider
region, codec cost, tenant policy, and failure domain.

The worker emits normalized events tagged with `tenant_id`, `assistant_id`, `call_id`,
`turn_id`, and `response_id`. Kafka partitions primarily by `call_id` for per-call
ordering. Tenant identity remains metadata for billing, quotas, access control, and
observability.

## Isolation And Quotas

Production controls should include:

- per-tenant and per-assistant concurrent-call limits;
- token, audio-minute, and tool-execution budgets;
- provider-specific quota allocation and rate limiting;
- bounded queues and admission control before worker saturation;
- fair scheduling so one tenant cannot consume every provider connection;
- tenant-aware Kafka, database, and observability cardinality limits; and
- stronger deployment, key, or data isolation for enterprise tenants.

Large tenants may need dedicated provider projects, Kafka quotas, worker pools,
encryption keys, or regional data planes. `tenant_id` should never be treated as the
only security boundary; authorization must be enforced at every API and consumer.

## Failure Domains

A worker failure should affect its assigned calls, not the whole fleet. A Kafka
partition issue should delay asynchronous consumers without entering live media
handoffs. A Postgres outage should block configuration changes and durable writes but
not insert a database round trip between active STT, LLM, and TTS stages. A Temporal
worker outage should delay outer-loop activities without stopping audio.

Capacity planning must include provider connection limits, egress bandwidth, audio
transcoding CPU, Kafka partition count, Postgres pool capacity, and metrics cardinality.

## Customer Server URLs And Webhooks

The customer server URL is a public integration boundary:

- Live, fast, read-only tool calls may use direct HTTP with a strict deadline,
  idempotency key, authentication, and cancellation.
- State-changing or long-running tools should publish a request and use an idempotent
  worker or Temporal workflow.
- End-of-call reports should be queued after call completion and delivered
  asynchronously.
- Delivery attempts, response codes, next retry time, and terminal status belong in
  Postgres.
- Webhook signatures and replay protection must be tenant-specific.

Internal Kafka events are not customer webhooks. Temporal activities are not customer
APIs. Kafka and Temporal coordinate platform work; the webhook is the tenant-facing
HTTP contract.

## When Temporal Is Worth It

Temporal is useful when a tool or webhook flow has multiple durable steps, long waits,
retry schedules, compensation, or a customer-visible state machine. It is not required
for every call or every asynchronous task.

Examples that justify a durable workflow:

- retry an end-of-call webhook for hours with backoff;
- wait for an external job and then finalize billing;
- coordinate a state-changing tool across multiple systems;
- generate summary, evaluation, and compliance artifacts with partial retry; or
- resume a customer action after a worker deployment.

A single idempotent Kafka consumer may be simpler for one-step persistence or analytics.
The architecture should select Temporal because the workflow semantics are needed, not
because it is present in the stack.
