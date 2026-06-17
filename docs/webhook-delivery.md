# Webhook Delivery

Webhook delivery is a customer-facing integration boundary, not an internal Kafka or
Temporal API. VoiceMesh now has a reusable `WebhookDeliveryWorkflow` for end-of-call or
tool-result delivery.

The workflow:

- persists the delivery row;
- posts to the customer URL from an activity;
- sends `Idempotency-Key`;
- records every attempt;
- retries with backoff;
- marks `DELIVERED` or `FAILED`.

External delivery is at-least-once. Exactly-once delivery is not claimed. Customer
endpoints must use the idempotency key to dedupe repeated requests.

Local mock endpoint:

```text
POST /mock-customer/webhook-sink
```

Production hardening still needed:

- tenant-specific signing secrets;
- replay protection;
- per-tenant retry policies;
- dead-letter and manual replay UI;
- webhook event schemas and compatibility guarantees.
