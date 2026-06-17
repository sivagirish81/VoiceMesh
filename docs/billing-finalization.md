# Billing Finalization

Live metering remains outside Temporal. The session worker emits coarse usage events to
Kafka. The event worker writes usage to Postgres idempotently, then signals
`BillingFinalizationWorkflow`.

```text
usage event -> Kafka -> UsageWriter/Postgres -> UsageRecorded signal
```

The signal is only a wake-up hint. Postgres is the source of truth. Duplicate Kafka
events are protected by idempotency keys and unique usage rows, and duplicate Temporal
signals are safe.

The workflow states are:

```text
WAITING_FOR_CALL_END
WAITING_FOR_USAGE
FINALIZING
FINALIZED
FINALIZED_WITH_WARNINGS
FAILED
NEEDS_REVIEW
```

For the local demo, missing usage waits for `BILLING_USAGE_WAIT_SECONDS` and then follows
`BILLING_MISSING_USAGE_POLICY`.

Demo:

```bash
make demo-billing-late-tts
```

Expected behavior:

```text
call.ended arrives
STT/LLM usage exists
TTS usage is missing
workflow waits
tts usage event arrives later
event worker writes Postgres
event worker signals workflow
workflow finalizes final_call_billing_records
```
