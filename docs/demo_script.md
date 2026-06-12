# Three-Minute Demo Script

## 0:00-0:35 — Normal Voice Call

Open `http://localhost:3000/demo`. Start the microphone and say:

> Explain why backpressure matters in a live voice pipeline.

Pause. Point out real VAD speech boundaries, the final transcript, streamed response,
and browser audio playback.

## 0:35-1:00 — Pipeline Visibility

Show the live event feed and queue depths. Open Jaeger and select the
`voicemesh-api` service. Explain that provider, Kafka, Postgres, WebSocket, and stage
operations share a trace context where possible.

Open Kafka UI and show `pipeline-events`. Open Temporal UI and show `call-<call_id>`.
Emphasize that tokens are in Kafka but not Temporal.

## 1:00-1:35 — TTS Backpressure

Run:

```bash
make demo-tts-backpressure
```

Ask a question that produces several sentences. Show `llm_to_tts` growing, the pipeline
entering `corked`, and `pipeline.corked` in the event feed. Reset injection in the
dashboard. Show the queue drain and `pipeline.uncorked`.

## 1:35-1:55 — Duplicate Delivery

Run:

```bash
make demo-duplicate-events
```

Open the call detail page. Show `duplicate_event.ignored`. Explain that the original
idempotency key cannot create a second state transition or outbox row.

## 1:55-2:25 — Postgres Failure

Start another call, then run:

```bash
make demo-db-down
```

Keep speaking. Show Kafka and the live media path continuing while DB write retries and
failure metrics are visible. State the limitation clearly: writes that exhaust retries
are visible but not automatically reconstructed in this POC.

## 2:25-3:00 — Temporal Worker Recovery

During a call, run:

```bash
make demo-kill-worker
```

Show the worker stop and restart. Refresh Temporal UI and show that the workflow
history survived and processing continued. Close with the design split: Kafka for
throughput and replay, Temporal for durable lifecycle, Postgres for idempotent query
state, and OTel for correlated evidence.

