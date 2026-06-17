# Durable Action Tools

VoiceMesh models tool execution with three modes:

```text
SYNC_DIRECT  -> fast read-only hot-path API call
ASYNC_JOB    -> lightweight accepted/pending event
DURABLE_ACTION -> Temporal workflow for state-changing work
```

Do not route every tool through Temporal. A lookup that must answer the caller in a few
hundred milliseconds should stay in the session worker. A refund, booking,
cancellation, claim, or dispatch request belongs in `DurableActionWorkflow`.

## Cancel Before External ID

The workflow explicitly handles this race:

```text
create external action starts
cancel signal arrives before external_request_id exists
workflow records cancel_requested=true
create returns external_request_id
workflow immediately calls cancel endpoint
workflow finalizes CANCELLED or CANNOT_CANCEL
```

Demo:

```bash
make demo-durable-action-cancel
```

Evidence:

- Temporal UI shows `SignalReceived CancelRequested` before the create activity
  completes.
- Postgres `tool_invocations` stores `status=CANCELLED` and
  `external_request_id=rr_001`.
- `tool_invocation_attempts` contains create and cancel attempts.
- Kafka receives `tool.action.*` events via the outbox publisher.
