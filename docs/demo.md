# Durable Outer-Loop Demos

Start the stack:

```bash
make up
make migrate
```

## Durable Action Cancel Race

```bash
make demo-durable-action-cancel
```

Open Temporal UI at `http://localhost:8080` and search for workflow id prefix `tool-`.

Look for:

- create activity scheduled;
- `CancelRequested` signal received while create is sleeping;
- create returns `rr_001`;
- cancel activity runs immediately;
- workflow completes as `CANCELLED`.

Check Postgres:

```bash
docker compose exec -T postgres psql -U postgres -d voice_lab \
  -c "select tool_invocation_id,status,external_request_id,cancel_requested from tool_invocations order by created_at desc limit 5;"
```

## Billing Waits For Late TTS Usage

```bash
make demo-billing-late-tts
```

Open Temporal UI and search for workflow id prefix `billing-`.

Look for:

- workflow waiting for usage;
- `UsageRecorded` signal after TTS usage arrives;
- final billing activity completion.

Check Postgres:

```bash
docker compose exec -T postgres psql -U postgres -d voice_lab \
  -c "select call_id,status,total_cost_cents,warnings from final_call_billing_records order by finalized_at desc limit 5;"
```

Jaeger should show `temporal.activity.*` spans for activities. Kafka UI should show
tool, billing, and workflow events on the relevant topics.

## Hot-Path Barge-In Demos

These demos are browser-guided because the current POC transport is the dashboard
WebSocket microphone. They exercise the hot path only; normal barge-in does not signal
Temporal.

```bash
make demo-barge-in-confirmed
make demo-barge-in-noise-rejected
make demo-barge-in-backchannel
```

Open `http://localhost:3000/demo`, run the guided steps, and watch:

- the Barge-in dashboard card;
- event feed entries for `user.barge_in_candidate`, `user.barge_in_confirmed`,
  `user.barge_in_rejected`, and `user.barge_in_classified`;
- `pipeline.response_cancelled` for confirmed interruptions;
- Jaeger spans named `barge_in.candidate`, `barge_in.confirmation`,
  `barge_in.cancel_response`, and `barge_in.semantic_resolution`.

POC limitation: exact browser audio resume after a rejected candidate is best-effort,
not sample-perfect.
