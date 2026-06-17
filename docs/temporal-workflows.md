# Temporal Workflows

VoiceMesh uses Temporal only in the durable outer loop. It is not in the live media
path:

```text
Transport -> Session Worker -> VAD -> STT -> LLM -> TTS -> Transport
```

The session worker still owns active provider streams, token/audio queues,
backpressure, cancellation, and eventual barge-in behavior. Temporal owns durable work
that can outlive the call or needs retry/audit semantics:

```text
Durable tool actions
Post-call billing finalization
Webhook delivery retries
Call completion state
Workflow completion events
```

## Workflows

`DurableActionWorkflow`

- starts state-changing external actions;
- accepts cancellation before or after the external request ID exists;
- calls create/cancel/status APIs from activities;
- persists tool state in Postgres;
- emits tool events through the outbox/Kafka path.

`BillingFinalizationWorkflow`

- starts after `call.ended` or after usage arrives;
- treats Postgres usage rows as source of truth;
- waits for required usage types such as TTS;
- finalizes into `final_call_billing_records`;
- emits `billing.finalized`.

`WebhookDeliveryWorkflow`

- posts an event payload to a customer URL;
- uses idempotency headers;
- records every attempt;
- retries with backoff;
- marks `DELIVERED` or `FAILED`.

`CallCompletionWorkflow`

- coordinates post-call summary, billing finalization, final call state, and
  `workflow.done`.

## Why Temporal Helps

Temporal replaces hand-rolled job tables, retry loops, timeout bookkeeping, and
"resume from step N" recovery code. External APIs and webhooks remain at-least-once side
effects; idempotency keys make retries safe where the customer endpoint cooperates.
