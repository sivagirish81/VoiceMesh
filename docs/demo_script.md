# Three-Minute Demo Script

The demo shows a real local vertical slice and then explains how the production
architecture separates the hot path from durable systems.

## 0:00-0:35 - Normal Voice Call

Open `http://localhost:3000/demo`. Start the microphone and say:

> Explain why backpressure matters in a live voice pipeline.

Pause. Point out real PCM capture, RMS VAD, a finalized OpenAI STT transcript, streaming
OpenAI LLM output, phrase-level OpenAI TTS, and browser playback.

Be precise: the POC buffers one user turn before STT. Production direction is a
long-lived streaming STT adapter that receives frames continuously.

## 0:35-1:00 - Runtime And Observability

Show queue depths and the event feed. Open Jaeger and select `voicemesh-api`.

Explain:

- the live handoff is in memory inside `StreamModule`;
- Kafka, Postgres, and Temporal are adjacent;
- the POC emits unusually fine-grained Kafka events for lab visibility; and
- production would retain coarse milestones and metrics, not every token/chunk.

## 1:00-1:35 - TTS Backpressure

Run:

```bash
make demo-tts-backpressure
```

No microphone is required. The command creates real spoken input through OpenAI,
injects 400 ms TTS chunk delay, and shows the bounded `llm_to_tts` queue grow. At the
high watermark, the session runtime corks upstream production. After delay removal, the
queue drains to the low watermark and uncorks.

Emphasize that cork/uncork is an in-memory scheduling decision. Kafka records it for
this reliability demo; Temporal is not required for normal backpressure.

## 1:35-1:55 - Duplicate Delivery

Run:

```bash
make demo-duplicate-events
```

Show `duplicate_event.ignored` on the call page. Explain that at-least-once systems need
idempotent consumers and deterministic keys. Also state the current limitation: the POC
can publish a critical event directly and through its outbox, so a production version
must establish one authoritative path per logical event.

## 1:55-2:25 - Postgres Failure

Run:

```bash
make demo-db-down
```

Show Kafka and the in-memory pipeline continuing where possible while DB failures become
visible. State that the current session runtime still awaits bounded repository retries.
The production target moves persistence behind Kafka consumers/outbox workers so
Postgres is not between live stages.

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
