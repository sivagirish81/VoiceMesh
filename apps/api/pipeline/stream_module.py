import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from contextlib import suppress
from typing import Any
from uuid import uuid4

from fastapi import WebSocketDisconnect
from opentelemetry import trace

from apps.api.config import Settings
from apps.api.db.repository import PostgresRepository
from apps.api.events.kafka_producer import KafkaEventProducer
from apps.api.events.schemas import EventType, PipelineEvent
from apps.api.pipeline.backpressure import BackpressureController
from apps.api.pipeline.events import AudioFrame, FinalizedTurn, PipelineState
from apps.api.pipeline.stages.llm import generate_tokens
from apps.api.pipeline.stages.stt import transcribe_turn
from apps.api.pipeline.stages.tts import synthesize_audio
from apps.api.pipeline.stages.vad import EnergyVADProvider, TurnDetector
from apps.api.providers.base import LLMProvider, STTProvider, TTSProvider
from apps.api.telemetry.metrics import ACTIVE_CALLS, PROVIDER_FAILURES, STAGE_LATENCY
from apps.api.telemetry.tracing import current_trace_id
from apps.api.temporal_client import TemporalLifecycleClient
from apps.api.websocket_transport import BrowserWebSocketTransport

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)
QueueCallback = Callable[[PipelineState], Awaitable[None]]


class StreamModule:
    def __init__(
        self,
        *,
        call_id: str,
        settings: Settings,
        transport: BrowserWebSocketTransport,
        stt: STTProvider,
        llm: LLMProvider,
        tts: TTSProvider,
        producer: KafkaEventProducer,
        repository: PostgresRepository,
        temporal: TemporalLifecycleClient,
    ) -> None:
        self.call_id = call_id
        self.settings = settings
        self.transport = transport
        self.stt = stt
        self.llm = llm
        self.tts = tts
        self.producer = producer
        self.repository = repository
        self.temporal = temporal
        self.state = PipelineState(call_id=call_id)
        self.vad = EnergyVADProvider(settings.vad_energy_threshold)
        self.turn_detector = TurnDetector(settings.vad_silence_ms)
        self._audio_buffer = bytearray()
        self._sample_rate = 16000
        self._turn_lock = asyncio.Lock()
        self._closed = False

    async def run(self) -> None:
        ACTIVE_CALLS.inc()
        await self.transport.accept()
        try:
            await self.repository.create_call(
                self.call_id, self.stt.name, self.llm.name, self.tts.name
            )
            await self.temporal.start_call(self.call_id)
            await self.emit(
                EventType.CALL_STARTED,
                "transport",
                turn_id="session",
                payload={
                    "providers": {
                        "stt": self.stt.name,
                        "llm": self.llm.name,
                        "tts": self.tts.name,
                    }
                },
                critical=True,
            )
            await self.transport.send_json("call.started", call_id=self.call_id)
            async for message in self.transport.receive_messages():
                if isinstance(message, AudioFrame):
                    await self._handle_audio(message)
                elif message.get("type") == "audio.end_turn":
                    await self._finalize_turn()
                elif message.get("type") == "client.ping":
                    await self.transport.send_json("client.pong")
                elif message.get("type") == "call.end":
                    break
        except WebSocketDisconnect:
            logger.info("browser disconnected", extra={"call_id": self.call_id})
        except Exception as exc:
            logger.exception("call pipeline failed", extra={"call_id": self.call_id})
            with suppress(Exception):
                await self._fail_call(str(exc))
        finally:
            await self._end_call()
            ACTIVE_CALLS.dec()

    async def _handle_audio(self, frame: AudioFrame) -> None:
        if len(self._audio_buffer) + len(frame.data) > self.settings.websocket_max_audio_bytes:
            raise ValueError("Audio turn exceeded maximum size")
        self._sample_rate = frame.sample_rate
        chunk_duration_ms = len(frame.data) / (2 * frame.sample_rate) * 1000
        with tracer.start_as_current_span("pipeline.vad") as span:
            speech = await self.vad.detect_speech(frame.data)
            span.set_attribute("call_id", self.call_id)
            span.set_attribute("speech", speech)
        started, ended = self.turn_detector.update(speech, chunk_duration_ms)
        if started:
            self._audio_buffer.clear()
            self.state.turn_id = str(uuid4())
            await self.emit(EventType.VAD_SPEECH_STARTED, "vad")
        if self.turn_detector.speaking or ended:
            self._audio_buffer.extend(frame.data)
        if ended:
            await self.emit(EventType.VAD_SPEECH_ENDED, "vad")
            await self._finalize_turn()

    async def _finalize_turn(self) -> None:
        if not self._audio_buffer or self._turn_lock.locked():
            return
        turn = FinalizedTurn(
            turn_id=self.state.turn_id,
            pcm_bytes=bytes(self._audio_buffer),
            sample_rate=self._sample_rate,
        )
        self._audio_buffer.clear()
        await self._process_turn(turn)

    async def _process_turn(self, turn: FinalizedTurn) -> None:
        async with self._turn_lock:
            self.state.turn_id = turn.turn_id
            transcript = await self._run_stt(turn)
            if not transcript.strip():
                return
            self.state.transcript = transcript
            await self.transport.send_json(
                "transcript.final", call_id=self.call_id, turn_id=turn.turn_id, text=transcript
            )
            await self._run_llm_tts_transport(transcript)

    async def _run_stt(self, turn: FinalizedTurn) -> str:
        self.state.current_stage = "stt"
        await self.emit(EventType.STT_STARTED, "stt")
        started = time.perf_counter()
        try:
            with tracer.start_as_current_span("pipeline.stt") as span:
                span.set_attribute("call_id", self.call_id)
                span.set_attribute("turn_id", turn.turn_id)
                span.set_attribute("provider", self.stt.name)
                transcript = await transcribe_turn(
                    self.stt,
                    turn.pcm_bytes,
                    turn.sample_rate,
                    self.settings.turn_timeout_seconds,
                )
        except TimeoutError:
            await self.emit(EventType.PIPELINE_STAGE_TIMEOUT, "stt")
            raise
        except Exception as exc:
            await self._provider_failed("stt", self.stt.name, exc)
            raise
        latency = (time.perf_counter() - started) * 1000
        STAGE_LATENCY.labels("stt", self.stt.name).observe(latency)
        await self.repository.record_metric(
            self.call_id, turn.turn_id, "stt", latency, 0, self.state.corked, self.stt.name
        )
        await self.emit(
            EventType.STT_FINAL_TRANSCRIPT,
            "stt",
            payload={"transcript": transcript, "latency_ms": latency},
            critical=True,
        )
        return transcript

    async def _run_llm_tts_transport(self, transcript: str) -> None:
        token_queue: BackpressureController[str | None] = BackpressureController(
            call_id=self.call_id,
            stage="llm_to_tts",
            high_watermark=self.settings.backpressure_high_watermark,
            low_watermark=self.settings.backpressure_low_watermark,
            on_state_change=self._on_backpressure,
        )
        audio_queue: BackpressureController[bytes | None] = BackpressureController(
            call_id=self.call_id,
            stage="tts_to_transport",
            high_watermark=self.settings.backpressure_high_watermark,
            low_watermark=self.settings.backpressure_low_watermark,
            on_state_change=self._on_backpressure,
        )
        llm_task = asyncio.create_task(self._produce_llm(transcript, token_queue))
        tts_task = asyncio.create_task(self._consume_tts(token_queue, audio_queue))
        transport_task = asyncio.create_task(self._consume_transport(audio_queue))
        try:
            await asyncio.wait_for(
                asyncio.gather(llm_task, tts_task, transport_task),
                timeout=self.settings.turn_timeout_seconds * 3,
            )
        except TimeoutError:
            for task in (llm_task, tts_task, transport_task):
                task.cancel()
            await self.emit(EventType.PIPELINE_STAGE_TIMEOUT, self.state.current_stage)
            raise

    async def _produce_llm(
        self, transcript: str, queue: BackpressureController[str | None]
    ) -> None:
        self.state.current_stage = "llm"
        await self.emit(EventType.LLM_STARTED, "llm")
        started = time.perf_counter()
        first = True
        response_parts: list[str] = []
        try:
            with tracer.start_as_current_span("pipeline.llm"):
                async for token in generate_tokens(self.llm, transcript, {"call_id": self.call_id}):
                    if first:
                        first = False
                        await self.emit(EventType.LLM_FIRST_TOKEN, "llm")
                    response_parts.append(token)
                    await queue.put(token)
                    self._set_queue_depth("llm_to_tts", queue.depth)
                    await self.emit(
                        EventType.LLM_TOKEN,
                        "llm",
                        payload={"token": token, "queue_depth": queue.depth},
                    )
                    await self.transport.send_json("llm.token", text=token)
        except Exception as exc:
            await self._provider_failed("llm", self.llm.name, exc)
            raise
        finally:
            await queue.put(None)
        response = "".join(response_parts)
        self.state.response = response
        latency = (time.perf_counter() - started) * 1000
        STAGE_LATENCY.labels("llm", self.llm.name).observe(latency)
        await self.repository.record_metric(
            self.call_id,
            self.state.turn_id,
            "llm",
            latency,
            queue.depth,
            self.state.corked,
            self.llm.name,
        )
        await self.emit(
            EventType.LLM_FINAL_RESPONSE,
            "llm",
            payload={"response": response, "latency_ms": latency},
            critical=True,
        )

    async def _consume_tts(
        self,
        token_queue: BackpressureController[str | None],
        audio_queue: BackpressureController[bytes | None],
    ) -> None:
        self.state.current_stage = "tts"
        buffer = ""
        first_audio = True
        started = time.perf_counter()
        await self.emit(EventType.TTS_STARTED, "tts")
        try:
            while True:
                token = await token_queue.get()
                token_queue.task_done()
                self._set_queue_depth("llm_to_tts", token_queue.depth)
                if token is None:
                    if buffer.strip():
                        first_audio = await self._synthesize_phrase(
                            buffer, audio_queue, first_audio
                        )
                    break
                buffer += token
                if len(buffer) >= 120 or any(buffer.rstrip().endswith(mark) for mark in ".?!"):
                    first_audio = await self._synthesize_phrase(buffer, audio_queue, first_audio)
                    buffer = ""
        except Exception as exc:
            await self._provider_failed("tts", self.tts.name, exc)
            raise
        finally:
            await audio_queue.put(None)
        latency = (time.perf_counter() - started) * 1000
        STAGE_LATENCY.labels("tts", self.tts.name).observe(latency)
        await self.repository.record_metric(
            self.call_id,
            self.state.turn_id,
            "tts",
            latency,
            audio_queue.depth,
            self.state.corked,
            self.tts.name,
        )

    async def _synthesize_phrase(
        self,
        phrase: str,
        audio_queue: BackpressureController[bytes | None],
        first_audio: bool,
    ) -> bool:
        with tracer.start_as_current_span("pipeline.tts"):
            async for audio in synthesize_audio(self.tts, phrase):
                if first_audio:
                    first_audio = False
                    await self.emit(EventType.TTS_FIRST_AUDIO, "tts")
                await audio_queue.put(audio)
                self._set_queue_depth("tts_to_transport", audio_queue.depth)
                await self.emit(
                    EventType.TTS_AUDIO_CHUNK,
                    "tts",
                    payload={"bytes": len(audio), "queue_depth": audio_queue.depth},
                )
        return first_audio

    async def _consume_transport(
        self, audio_queue: BackpressureController[bytes | None]
    ) -> None:
        self.state.current_stage = "transport"
        while True:
            audio = await audio_queue.get()
            audio_queue.task_done()
            self._set_queue_depth("tts_to_transport", audio_queue.depth)
            if audio is None:
                return
            await self.transport.send_audio(audio)
            await self.emit(
                EventType.TRANSPORT_AUDIO_SENT,
                "transport",
                payload={"bytes": len(audio), "queue_depth": audio_queue.depth},
            )

    async def _on_backpressure(self, corked: bool, reason: str, depth: int) -> None:
        self.state.corked = corked
        self.state.cork_reason = reason if corked else None
        event_type = EventType.PIPELINE_CORKED if corked else EventType.PIPELINE_UNCORKED
        await self.emit(
            event_type,
            "backpressure",
            payload={"reason": reason, "queue_depth": depth},
            critical=True,
        )
        await self.repository.update_call_state(
            self.call_id, corked=corked, cork_reason=reason if corked else None
        )
        await self.temporal.signal(
            self.call_id,
            "pipeline_event",
            {"event_type": str(event_type), "reason": reason},
        )
        await self.transport.send_json(
            str(event_type), corked=corked, reason=reason, queue_depths=self.state.queue_depths
        )

    def _set_queue_depth(self, stage: str, depth: int) -> None:
        self.state.queue_depths[stage] = depth

    async def _provider_failed(self, stage: str, provider: str, exc: Exception) -> None:
        PROVIDER_FAILURES.labels(provider, stage).inc()
        await self.emit(
            EventType.PROVIDER_FAILED,
            stage,
            payload={"provider": provider, "error": str(exc)},
            critical=True,
        )
        await self.temporal.signal(
            self.call_id,
            "provider_failed",
            {"stage": stage, "provider": provider, "error": str(exc)},
        )

    async def emit(
        self,
        event_type: EventType,
        stage: str,
        *,
        turn_id: str | None = None,
        payload: dict[str, Any] | None = None,
        critical: bool = False,
    ) -> PipelineEvent:
        self.state.sequence_number += 1
        event = PipelineEvent.create(
            call_id=self.call_id,
            turn_id=turn_id or self.state.turn_id,
            event_type=event_type,
            stage=stage,
            sequence_number=self.state.sequence_number,
            payload=payload,
            trace_id=current_trace_id(),
        )
        await self.producer.publish(event)
        inserted = await self.repository.persist_event(event, critical=critical)
        if inserted is False:
            duplicate = PipelineEvent.create(
                call_id=self.call_id,
                turn_id=event.turn_id,
                event_type=EventType.DUPLICATE_EVENT_IGNORED,
                stage="idempotency",
                sequence_number=self.state.sequence_number + 1,
                payload={"duplicate_idempotency_key": event.idempotency_key},
            )
            self.state.sequence_number += 1
            await self.producer.publish(duplicate)
        elif inserted is None:
            self.state.sequence_number += 1
            db_failure = PipelineEvent.create(
                call_id=self.call_id,
                turn_id=event.turn_id,
                event_type=EventType.POSTGRES_WRITE_FAILED,
                stage="postgres",
                sequence_number=self.state.sequence_number,
                payload={
                    "failed_event_type": str(event.event_type),
                    "idempotency_key": event.idempotency_key,
                },
                trace_id=current_trace_id(),
            )
            await self.producer.publish(db_failure)
            await self.transport.send_json(
                "pipeline.event",
                event=db_failure.model_dump(mode="json"),
                state={
                    "stage": stage,
                    "corked": self.state.corked,
                    "cork_reason": self.state.cork_reason,
                    "queue_depths": self.state.queue_depths,
                },
            )
        await self.repository.update_call_state(self.call_id, stage=stage)
        await self.transport.send_json(
            "pipeline.event",
            event=event.model_dump(mode="json"),
            state={
                "stage": stage,
                "corked": self.state.corked,
                "cork_reason": self.state.cork_reason,
                "queue_depths": self.state.queue_depths,
            },
        )
        return event

    async def _fail_call(self, error: str) -> None:
        await self.emit(
            EventType.CALL_FAILED,
            self.state.current_stage,
            turn_id="session",
            payload={"error": error},
            critical=True,
        )
        await self.repository.update_call_state(
            self.call_id, status="CALL_FAILED", error=error, ended=True
        )
        await self.temporal.signal(self.call_id, "call_failed", {"error": error})
        await self.transport.send_json("error", message=error)

    async def _end_call(self) -> None:
        if self._closed:
            return
        self._closed = True
        with suppress(Exception):
            await self.emit(
                EventType.CALL_ENDED,
                "transport",
                turn_id="session",
                payload={"final_response": self.state.response},
                critical=True,
            )
            await self.repository.update_call_state(
                self.call_id,
                status="CALL_COMPLETED",
                final_summary=self.state.response,
                ended=True,
            )
            await self.temporal.signal(
                self.call_id,
                "call_completed",
                {"summary": self.state.response},
            )
            await self.transport.send_json("call.ended", call_id=self.call_id)
