# Three-Minute Demo Script

The demo shows a real local vertical slice and then explains how the production
architecture separates the hot path from durable systems.

## 0:00-0:35 - Normal Voice Call

Open `http://localhost:3000/demo`. Start the microphone and say:

> Explain why backpressure matters in a live voice pipeline.

Pause. Point out real PCM capture, browser noise controls, WebRTC VAD debug state,
OpenAI Realtime STT deltas before the final turn, streaming OpenAI LLM output,
phrase-level OpenAI TTS, and browser playback.

## 0:35-1:00 - Runtime And Observability

Show weighted queue depths and the event feed. Open Jaeger and select `voicemesh-api`.

Explain:

- the live handoff is in memory inside `StreamModule`;
- Kafka, Postgres, and Temporal are adjacent;
- Kafka carries coarse milestones and usage, not raw frames, LLM tokens, or TTS chunks;
- the event worker projects those facts into Postgres outside the hot path; and
- the billing page shows the provider-cost and platform-fee rollup.

## 1:00-1:35 - TTS Backpressure

Run:

```bash
make demo-tts-backpressure
```

No microphone is required. The command creates real spoken input through OpenAI,
injects 400 ms TTS chunk delay, and shows `llm_to_tts` queued speak-ahead milliseconds
grow. At the high watermark, the session runtime corks upstream production. After delay
removal, the speak-ahead budget drains to the low watermark and uncorks.

Emphasize that cork/uncork is an in-memory scheduling decision. Kafka records it for
this reliability demo; Temporal is not required for normal backpressure.

## 1:35-1:55 - Duplicate Delivery

Run:

```bash
make demo-duplicate-events
```

Show `duplicate_event.ignored` on the call page. Explain that at-least-once systems need
idempotent consumers and deterministic keys. Point out that session facts publish
directly, while DB-derived billing facts use the outbox as their authoritative route.

## 1:55-2:25 - Postgres Failure

Run:

```bash
make demo-db-down
```

Show Kafka and the in-memory pipeline continuing while the event worker retries without
committing its offset. Resume Postgres and show delayed call/billing records appear.

## 2:25-3:00 - Temporal Worker Recovery

Run:

```bash
make demo-kill-worker
```

Show workflow history surviving worker restart in Temporal UI. Frame this honestly:
the demo proves durable outer-loop execution, not recovery of an active media socket.

Close with the target split:

- session worker for live media, cancellation, turn fencing, and backpressure;
- Kafka for coarse durable events and fanout;
- Postgres for system-of-record state and idempotency;
- Temporal only for lifecycle work that needs durable retries or long-running state; and
- OpenTelemetry for evidence across both synchronous and asynchronous boundaries.
