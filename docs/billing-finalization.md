# Billing Finalization

Live metering remains outside Temporal. The session worker emits coarse usage events to
Kafka during the call, then emits `usage.finalization_barrier` on `usage-events` at call
end. The existing event worker is the logical `UsageWriter`: it writes usage, the
manifest, per-turn expectations, and projection watermarks to Postgres idempotently.

```text
usage events -> Kafka -> UsageWriter/Postgres
call.ended -> BillingFinalizationWorkflow
usage.finalization_barrier -> manifest + projection watermark
```

`BillingFinalizationWorkflow` starts from `call.ended`, but it does not trust `call.ended`
as proof that usage has reached Postgres. It waits until:

- the usage manifest exists;
- the `projection_watermarks` row has caught up to the manifest's Kafka topic/partition
  offset;
- every expected `turn_id + usage_type` row has matching normalized usage in Postgres.

`UsageProjectionUpdated` signals are only wake-up hints after successful DB writes.
Postgres is the source of truth. Duplicate Kafka events are protected by idempotency keys
and unique usage rows, and duplicate Temporal signals are safe.

The workflow states are:

```text
WAITING_FOR_CALL_END
WAITING_FOR_MANIFEST
WAITING_FOR_PROJECTION
WAITING_FOR_USAGE
FINALIZING
FINALIZED
FINALIZED_WITH_WARNINGS
FAILED
NEEDS_REVIEW
```

For the local demo, missing usage waits for `BILLING_USAGE_WAIT_SECONDS`, uses
`BILLING_USAGE_SETTLE_SECONDS` between projection checks, and then follows
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
workflow waits for manifest/projection/usage
tts usage event arrives later
event worker writes Postgres and advances watermark
event worker signals UsageProjectionUpdated
workflow finalizes final_call_billing_records
```

If usage arrives after `final_call_billing_records` already exists, the UsageWriter starts
`BillingAdjustmentWorkflow`, which recomputes from `usage_records` and writes an immutable
`billing_adjustments` row instead of mutating the original usage ledger.

For historical billing analytics, PeerDB can replicate only the committed billing tables
from Postgres into ClickHouse Cloud:

```text
call_usage_events
billing_line_items
final_call_billing_records
billing_adjustments
```

The application does not dual-write authoritative billing rows to ClickHouse. Postgres
remains the ledger; ClickHouse is an eventually consistent projection for Grafana.
