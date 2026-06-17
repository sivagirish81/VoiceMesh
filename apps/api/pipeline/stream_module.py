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
from apps.api.events.kafka_producer import KafkaEventProducer
from apps.api.events.schemas import EventType, PipelineEvent
from apps.api.pipeline.backpressure import BackpressureController
from apps.api.pipeline.events import AudioFrame, PipelineState
from apps.api.pipeline.stages.llm import generate_tokens
from apps.api.pipeline.stages.tts import synthesize_audio
from apps.api.pipeline.stages.vad import EnergyVADProvider, TurnDetector
from apps.api.providers.base import (
    LLMProvider,
    StreamingSTTSession,
    STTProvider,
    TTSProvider,
)
from apps.api.telemetry.metrics import ACTIVE_CALLS, PROVIDER_FAILURES, STAGE_LATENCY
from apps.api.telemetry.tracing import current_trace_id, set_span_attributes
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
        temporal: TemporalLifecycleClient,
    ) -> None:
        self.call_id = call_id
        self.settings = settings
        self.transport = transport
        self.stt = stt
        self.llm = llm
        self.tts = tts
        self.producer = producer
        self.temporal = temporal
        self.state = PipelineState(call_id=call_id)
        self.vad = EnergyVADProvider(settings.vad_energy_threshold)
        self.turn_detector = TurnDetector(settings.vad_silence_ms)
        self._stt_session: StreamingSTTSession | None = None
        self._turn_has_audio = False
        self._turn_audio_bytes = 0
        self._turn_lock = asyncio.Lock()
        self._closed = False
        self._failed = False
        self._call_started_at = time.monotonic()
        self._usage_expectations: dict[str, set[str]] = {}

    async def run(self) -> None:
        ACTIVE_CALLS.inc()
        await self.transport.accept()
        with tracer.start_as_current_span("voice.call") as span:
            set_span_attributes(
                span,
                call_id=self.call_id,
                stt_provider=self.stt.name,
                llm_provider=self.llm.name,
                tts_provider=self.tts.name,
                stt_model=self.stt.model,
                llm_model=self.llm.model,
                tts_model=self.tts.model,
            )
            try:
                self._stt_session = await self.stt.open_stream(self._on_stt_delta)
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
                        },
                        "models": {
                            "stt": self.stt.model,
                            "llm": self.llm.model,
                            "tts": self.tts.model,
                        },
                    },
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
                span.record_exception(exc)
                span.set_attribute("error", True)
                logger.exception("call pipeline failed", extra={"call_id": self.call_id})
                with suppress(Exception):
                    await self._fail_call(str(exc))
            finally:
                set_span_attributes(
                    span,
                    final_stage=self.state.current_stage,
                    corked=self.state.corked,
                    final_response_chars=len(self.state.response),
                    duration_seconds=time.monotonic() - self._call_started_at,
                    failed=self._failed,
                )
                await self._end_call()
                if self._stt_session:
                    with suppress(Exception):
                        await self._stt_session.close()
                ACTIVE_CALLS.dec()

    async def _handle_audio(self, frame: AudioFrame) -> None:
        chunk_duration_ms = len(frame.data) / (2 * frame.sample_rate) * 1000
        with tracer.start_as_current_span("pipeline.vad") as span:
            speech = await self.vad.detect_speech(frame.data)
            set_span_attributes(
                span,
                call_id=self.call_id,
                turn_id=self.state.turn_id,
                stage="vad",
                speech=speech,
                audio_bytes=len(frame.data),
                sample_rate=frame.sample_rate,
                chunk_duration_ms=chunk_duration_ms,
            )
        started, ended = self.turn_detector.update(speech, chunk_duration_ms)
        if started:
            self.state.turn_id = str(uuid4())
            self._turn_has_audio = False
            self._turn_audio_bytes = 0
            await self.emit(EventType.VAD_SPEECH_STARTED, "vad")
        if self.turn_detector.speaking or ended:
            self._turn_audio_bytes += len(frame.data)
            if self._turn_audio_bytes > self.settings.websocket_max_audio_bytes:
                raise ValueError("Audio turn exceeded maximum size")
            if not self._stt_session:
                raise RuntimeError("Streaming STT session is not available")
            self._turn_has_audio = True
            await self._stt_session.append_audio(frame.data, frame.sample_rate)
        if ended:
            await self.emit(EventType.VAD_SPEECH_ENDED, "vad")
            await self._finalize_turn()

    async def _finalize_turn(self) -> None:
        if not self._turn_has_audio or self._turn_lock.locked():
            return
        turn_id = self.state.turn_id
        self._turn_has_audio = False
        self._turn_audio_bytes = 0
        await self._process_turn(turn_id)

    async def _process_turn(self, turn_id: str) -> None:
        async with self._turn_lock:
            self.state.turn_id = turn_id
            transcript = await self._run_stt(turn_id)
            if not transcript.strip():
                return
            self.state.transcript = transcript
            await self.transport.send_json(
                "transcript.final", call_id=self.call_id, turn_id=turn_id, text=transcript
            )
            await self._run_llm_tts_transport(transcript)

    async def _run_stt(self, turn_id: str) -> str:
        self.state.current_stage = "stt"
        await self.emit(EventType.STT_STARTED, "stt")
        started = time.perf_counter()
        try:
            with tracer.start_as_current_span("pipeline.stt") as span:
                set_span_attributes(
                    span,
                    call_id=self.call_id,
                    turn_id=turn_id,
                    stage="stt",
                    provider=self.stt.name,
                    model=self.stt.model,
                    timeout_seconds=self.settings.turn_timeout_seconds,
                )
                if not self._stt_session:
                    raise RuntimeError("Streaming STT session is not available")
                async with asyncio.timeout(self.settings.turn_timeout_seconds):
                    result = await self._stt_session.commit()
                set_span_attributes(
                    span,
                    transcript_chars=len(result.transcript),
                    audio_seconds=result.audio_seconds,
                    provider_item_id=result.item_id,
                )
        except TimeoutError:
            await self.emit(EventType.PIPELINE_STAGE_TIMEOUT, "stt")
            raise
        except Exception as exc:
            await self._provider_failed("stt", self.stt.name, exc)
            raise
        latency = (time.perf_counter() - started) * 1000
        STAGE_LATENCY.labels("stt", self.stt.name).observe(latency)
        await self.emit(
            EventType.STT_FINAL_TRANSCRIPT,
            "stt",
            payload={
                "transcript": result.transcript,
                "latency_ms": latency,
                "item_id": result.item_id,
                "audio_seconds": result.audio_seconds,
                "streaming": True,
                "model": self.stt.model,
            },
        )
        await self.emit(
            EventType.USAGE_STT_RECORDED,
            "stt",
            payload={
                "provider": self.stt.name,
                "model": self.stt.model,
                "measurements": [
                    {
                        "usage_type": "audio_minute",
                        "quantity": result.audio_seconds / 60,
                        "unit": "minute",
                        "estimated": False,
                    }
                ],
            },
        )
        self._expect_usage(turn_id, "stt_audio_seconds")
        return result.transcript

    async def _on_stt_delta(self, delta: str) -> None:
        await self.transport.send_json(
            "transcript.partial",
            call_id=self.call_id,
            turn_id=self.state.turn_id,
            delta=delta,
        )

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
            with tracer.start_as_current_span("pipeline.llm") as span:
                set_span_attributes(
                    span,
                    call_id=self.call_id,
                    turn_id=self.state.turn_id,
                    stage="llm",
                    provider=self.llm.name,
                    model=self.llm.model,
                    transcript_chars=len(transcript),
                )
                async for token in generate_tokens(self.llm, transcript, {"call_id": self.call_id}):
                    if first:
                        first = False
                        await self.emit(EventType.LLM_FIRST_TOKEN, "llm")
                    response_parts.append(token)
                    await queue.put(token)
                    self._set_queue_depth("llm_to_tts", queue.depth)
                    await self._send_pipeline_state()
                    await self.transport.send_json("llm.token", text=token)
                set_span_attributes(
                    span,
                    response_chars=sum(len(part) for part in response_parts),
                    max_queue_depth=max(self.state.queue_depths.get("llm_to_tts", 0), 0),
                )
        except Exception as exc:
            await self._provider_failed("llm", self.llm.name, exc)
            raise
        finally:
            await queue.put(None)
        response = "".join(response_parts)
        self.state.response = response
        latency = (time.perf_counter() - started) * 1000
        STAGE_LATENCY.labels("llm", self.llm.name).observe(latency)
        await self.emit(
            EventType.LLM_FINAL_RESPONSE,
            "llm",
            payload={"response": response, "latency_ms": latency},
        )
        usage = self.llm.consume_usage()
        await self.emit(
            EventType.USAGE_LLM_RECORDED,
            "llm",
            payload={
                "provider": self.llm.name,
                "model": self.llm.model,
                "measurements": [
                    {
                        "usage_type": "input_token",
                        "quantity": max(0, usage.input_tokens - usage.cached_input_tokens),
                        "unit": "token",
                        "estimated": False,
                    },
                    {
                        "usage_type": "cached_input_token",
                        "quantity": usage.cached_input_tokens,
                        "unit": "token",
                        "estimated": False,
                    },
                    {
                        "usage_type": "output_token",
                        "quantity": usage.output_tokens,
                        "unit": "token",
                        "estimated": False,
                    },
                ],
            },
        )
        self._expect_usage(self.state.turn_id, "llm_input_tokens")
        self._expect_usage(self.state.turn_id, "llm_output_tokens")

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
                await self._send_pipeline_state()
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
        await self.emit(
            EventType.TTS_COMPLETED,
            "tts",
            payload={"latency_ms": latency, "queue_depth": audio_queue.depth},
        )

    async def _synthesize_phrase(
        self,
        phrase: str,
        audio_queue: BackpressureController[bytes | None],
        first_audio: bool,
    ) -> bool:
        with tracer.start_as_current_span("pipeline.tts") as span:
            set_span_attributes(
                span,
                call_id=self.call_id,
                turn_id=self.state.turn_id,
                stage="tts",
                provider=self.tts.name,
                model=self.tts.model,
                phrase_chars=len(phrase),
            )
            audio_bytes = 0
            chunks = 0
            async for audio in synthesize_audio(self.tts, phrase):
                if first_audio:
                    first_audio = False
                    await self.emit(EventType.TTS_FIRST_AUDIO, "tts")
                await audio_queue.put(audio)
                audio_bytes += len(audio)
                chunks += 1
                self._set_queue_depth("tts_to_transport", audio_queue.depth)
                await self._send_pipeline_state()
            set_span_attributes(
                span,
                audio_bytes=audio_bytes,
                audio_chunks=chunks,
                queue_depth=audio_queue.depth,
            )
        usage = self.tts.consume_usage()
        await self.emit(
            EventType.USAGE_TTS_RECORDED,
            "tts",
            payload={
                "provider": self.tts.name,
                "model": self.tts.model,
                "audio_seconds": usage.output_audio_seconds,
                "audio_bytes": usage.output_audio_bytes,
                "measurements": [
                    {
                        "usage_type": "input_text_token",
                        "quantity": max(1, (len(usage.input_text) + 3) // 4),
                        "unit": "token",
                        "estimated": True,
                    },
                    {
                        "usage_type": "output_audio_token",
                        "quantity": round(usage.output_audio_seconds * 20),
                        "unit": "token",
                        "estimated": True,
                    },
                ],
            },
        )
        self._expect_usage(self.state.turn_id, "tts_characters")
        self._expect_usage(self.state.turn_id, "tts_audio_seconds")
        return first_audio

    async def _consume_transport(
        self, audio_queue: BackpressureController[bytes | None]
    ) -> None:
        self.state.current_stage = "transport"
        bytes_sent = 0
        chunks_sent = 0
        while True:
            audio = await audio_queue.get()
            audio_queue.task_done()
            self._set_queue_depth("tts_to_transport", audio_queue.depth)
            await self._send_pipeline_state()
            if audio is None:
                await self.emit(
                    EventType.TRANSPORT_AUDIO_SENT,
                    "transport",
                    payload={"bytes": bytes_sent, "chunks": chunks_sent, "queue_depth": 0},
                )
                return
            await self.transport.send_audio(audio)
            bytes_sent += len(audio)
            chunks_sent += 1

    async def _on_backpressure(self, corked: bool, reason: str, depth: int) -> None:
        self.state.corked = corked
        self.state.cork_reason = reason if corked else None
        event_type = EventType.PIPELINE_CORKED if corked else EventType.PIPELINE_UNCORKED
        with tracer.start_as_current_span("pipeline.backpressure") as span:
            set_span_attributes(
                span,
                call_id=self.call_id,
                turn_id=self.state.turn_id,
                stage="backpressure",
                corked=corked,
                reason=reason,
                queue_depth=depth,
            )
            await self.emit(
                event_type,
                "backpressure",
                payload={"reason": reason, "queue_depth": depth},
            )
        await self.transport.send_json(
            str(event_type), corked=corked, reason=reason, queue_depths=self.state.queue_depths
        )

    def _set_queue_depth(self, stage: str, depth: int) -> None:
        self.state.queue_depths[stage] = depth

    def _expect_usage(self, turn_id: str, usage_type: str) -> None:
        if turn_id == "session":
            return
        self._usage_expectations.setdefault(turn_id, set()).add(usage_type)

    def _usage_manifest_payload(self) -> dict[str, Any]:
        expected_turns = [
            {
                "turn_id": turn_id,
                "expected_usage": sorted(expected_usage),
            }
            for turn_id, expected_usage in sorted(self._usage_expectations.items())
            if expected_usage
        ]
        return {
            "tenant_id": "local-demo-tenant",
            "assistant_id": "local-demo-assistant",
            "pricing_version": self.settings.billing_pricing_version,
            "expected_turns": expected_turns,
        }

    async def _send_pipeline_state(self) -> None:
        await self.transport.send_json(
            "pipeline.state",
            state={
                "stage": self.state.current_stage,
                "corked": self.state.corked,
                "cork_reason": self.state.cork_reason,
                "queue_depths": self.state.queue_depths,
            },
        )

    async def _provider_failed(self, stage: str, provider: str, exc: Exception) -> None:
        PROVIDER_FAILURES.labels(provider, stage).inc()
        await self.emit(
            EventType.PROVIDER_FAILED,
            stage,
            payload={"provider": provider, "error": str(exc)},
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
        self._failed = True
        await self.emit(
            EventType.CALL_FAILED,
            self.state.current_stage,
            turn_id="session",
            payload={"error": error},
        )
        await self.temporal.signal(self.call_id, "call_failed", {"error": error})
        await self.transport.send_json("error", message=error)

    async def _end_call(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._failed:
            return
        with suppress(Exception):
            await self.emit(
                EventType.USAGE_FINALIZATION_BARRIER,
                "billing",
                turn_id="session",
                payload=self._usage_manifest_payload(),
            )
            await self.emit(
                EventType.CALL_ENDED,
                "transport",
                turn_id="session",
                payload={
                    "final_response": self.state.response,
                    "duration_seconds": time.monotonic() - self._call_started_at,
                },
            )
            await self.temporal.signal(
                self.call_id,
                "call_completed",
                {"summary": self.state.response},
            )
            await self.transport.send_json("call.ended", call_id=self.call_id)
