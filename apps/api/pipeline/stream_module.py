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
from apps.api.pipeline.backpressure import (
    BackpressureTransition,
    DepthUnit,
    FlowControlledQueue,
    QueueClosed,
)
from apps.api.pipeline.barge_in import (
    BargeInCandidate,
    BargeInCoordinator,
    BargeInSemantic,
    classify_interruption,
)
from apps.api.pipeline.events import (
    AudioChunk,
    AudioFrame,
    BackpressureStageState,
    EndOfStream,
    PipelineState,
    TextChunk,
)
from apps.api.pipeline.stages.llm import generate_tokens
from apps.api.pipeline.stages.tts import synthesize_audio
from apps.api.pipeline.stages.vad import (
    EnergyVADProvider,
    SileroVADProvider,
    SmoothedTurnDetector,
    TurnUpdate,
    VADState,
    WebRTCVADProvider,
)
from apps.api.providers.base import (
    LLMProvider,
    StreamingSTTSession,
    STTProvider,
    TTSProvider,
    VADProvider,
    VADResult,
)
from apps.api.telemetry.metrics import (
    ACTIVE_CALLS,
    AUDIO_PLAYED_AFTER_CANCEL_MS,
    BARGE_IN_BACKEND_CANCEL_LATENCY,
    BARGE_IN_CANDIDATES_TOTAL,
    BARGE_IN_CLASSIFICATION_LATENCY,
    BARGE_IN_CONFIRMATION_LATENCY,
    BARGE_IN_CONFIRMED_TOTAL,
    BARGE_IN_PLAYBACK_STOP_LATENCY,
    BARGE_IN_REJECTED_TOTAL,
    INTERRUPTED_RESPONSE_SPOKEN_RATIO,
    LLM_FIRST_TOKEN_LATENCY,
    PROVIDER_FAILURES,
    STAGE_LATENCY,
    STALE_AUDIO_DROPPED_MS_TOTAL,
    STALE_CHUNKS_DROPPED_TOTAL,
    STT_TURNS_COMMITTED_TOTAL,
    TTS_FIRST_AUDIO_LATENCY,
    VAD_ENDPOINT_DELAY,
    VAD_ENERGY,
    VAD_FRAMES_TOTAL,
    VAD_NOISE_FLOOR,
    VAD_NOISE_TURNS_IGNORED_TOTAL,
    VAD_STATE_TRANSITIONS_TOTAL,
    VAD_TURN_DURATION,
)
from apps.api.telemetry.tracing import current_trace_id, set_span_attributes
from apps.api.temporal_client import TemporalLifecycleClient
from apps.api.websocket_transport import BrowserWebSocketTransport

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)
QueueCallback = Callable[[PipelineState], Awaitable[None]]
TextQueueItem = TextChunk | EndOfStream
AudioQueueItem = AudioChunk | EndOfStream


class ResponseController:
    def __init__(self) -> None:
        self.active_response_id: str | None = None
        self.response_turns: dict[str, str] = {}
        self.cancelled: dict[str, str] = {}

    def start_response(self, turn_id: str) -> str:
        response_id = str(uuid4())
        self.active_response_id = response_id
        self.response_turns[response_id] = turn_id
        return response_id

    def cancel_response(self, response_id: str, reason: str) -> bool:
        if response_id in self.cancelled:
            return False
        self.cancelled[response_id] = reason
        if self.active_response_id == response_id:
            self.active_response_id = None
        return True

    def finish_response(self, response_id: str) -> bool:
        if self.active_response_id != response_id:
            return False
        self.active_response_id = None
        return True

    def is_response_cancelled(self, response_id: str) -> bool:
        return response_id in self.cancelled

    def is_response_active(self, response_id: str, turn_id: str) -> bool:
        return (
            self.active_response_id == response_id
            and self.response_turns.get(response_id) == turn_id
            and response_id not in self.cancelled
        )


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
        self.vad = self._create_vad_provider(settings)
        self.turn_detector = SmoothedTurnDetector(
            min_speech_ms=settings.vad_min_speech_ms,
            end_silence_ms=settings.vad_end_silence_ms,
            speech_pad_ms=settings.vad_speech_pad_ms,
        )
        self._stt_session: StreamingSTTSession | None = None
        self._turn_has_audio = False
        self._turn_audio_bytes = 0
        self._pending_vad_frames: list[AudioFrame] = []
        self._turn_stats_by_id: dict[str, dict[str, float]] = {}
        self._turn_lock = asyncio.Lock()
        self._closed = False
        self._failed = False
        self._call_started_at = time.monotonic()
        self._usage_expectations: dict[str, set[str]] = {}
        self._responses = ResponseController()
        self._live_queues: dict[str, FlowControlledQueue[Any]] = {}
        self._barge_in = BargeInCoordinator(
            call_id=call_id,
            candidate_retention_ms=settings.barge_in_candidate_retention_ms,
        )
        self._conversation: list[dict[str, Any]] = []
        self._response_text_by_id: dict[str, str] = {}
        self._response_estimated_ms_by_id: dict[str, float] = {}
        self._response_audio_sequences: dict[str, int] = {}
        self._response_first_audio_at: dict[str, float] = {}
        self._response_last_audio_sent_at: dict[str, float] = {}
        self._response_transport_complete: set[str] = set()
        self._playback_done_response_ids: set[str] = set()
        self._interrupted_response_ids: set[str] = set()
        self._last_interruption: dict[str, Any] | None = None

    def _create_vad_provider(self, settings: Settings) -> VADProvider:
        if settings.vad_provider == "webrtc":
            return WebRTCVADProvider(
                mode=settings.webrtc_vad_mode,
                sample_rate=settings.vad_sample_rate,
                frame_ms=settings.vad_frame_ms,
            )
        if settings.vad_provider == "energy":
            return EnergyVADProvider(
                settings.vad_energy_threshold,
                adaptive_noise_floor=settings.energy_vad_adaptive_noise_floor,
                noise_multiplier=settings.energy_vad_noise_multiplier,
                noise_update_alpha=settings.energy_vad_noise_update_alpha,
            )
        return SileroVADProvider()

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
                    elif message.get("type") == "client.barge_in_candidate":
                        await self._handle_barge_in_candidate(message)
                    elif message.get("type") == "playback.progress":
                        await self._handle_playback_progress(message)
                    elif message.get("type") == "playback.done":
                        await self._handle_playback_done(message)
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
        try:
            vad_result = await self.vad.detect_speech(frame.data, frame.sample_rate)
        except Exception as exc:
            await self.emit(
                EventType.VAD_PROVIDER_FAILED,
                "vad",
                payload={"provider": self.vad.name, "error": type(exc).__name__},
            )
            raise
        update = self.turn_detector.update(vad_result)
        self._record_vad_metrics(vad_result, update.previous_state, update.state)
        self._update_vad_state(vad_result, update)
        if update.state_changed or update.started or update.ended:
            with tracer.start_as_current_span("pipeline.vad") as span:
                set_span_attributes(
                    span,
                    call_id=self.call_id,
                    turn_id=self.state.turn_id,
                    stage="vad",
                    provider=self.vad.name,
                    vad_decision=vad_result.speech,
                    vad_energy=vad_result.energy,
                    vad_noise_floor=vad_result.noise_floor,
                    vad_probability=vad_result.probability,
                    vad_state=update.state.value,
                    audio_bytes=len(frame.data),
                    sample_rate=frame.sample_rate,
                    chunk_duration_ms=chunk_duration_ms,
                    speech_started=update.started,
                    speech_ended=update.ended,
                    speech_duration_ms=update.speech_duration_ms,
                    speech_frame_ratio=update.speech_frame_ratio,
                )
        if update.state_changed:
            await self.emit(
                EventType.VAD_STATE_CHANGED,
                "vad",
                payload={
                    "provider": self.vad.name,
                    "from_state": update.previous_state.value,
                    "to_state": update.state.value,
                    "speech": vad_result.speech,
                },
            )
        if update.state == VADState.QUIET and not update.ended:
            self._pending_vad_frames.clear()
            await self._maybe_reject_barge_in_candidate("noise_spike")
        if update.state == VADState.STARTING:
            self._pending_vad_frames.append(frame)
        if update.started:
            self.state.turn_id = str(uuid4())
            self._turn_has_audio = False
            self._turn_audio_bytes = 0
            if frame not in self._pending_vad_frames:
                self._pending_vad_frames.append(frame)
            for pending in self._pending_vad_frames:
                await self._append_turn_audio(pending)
            self._pending_vad_frames.clear()
            await self.emit(
                EventType.VAD_SPEECH_STARTED,
                "vad",
                payload={
                    "provider": self.vad.name,
                    "state": update.state.value,
                    "speech_duration_ms": update.speech_duration_ms,
                    "speech_frame_ratio": update.speech_frame_ratio,
                },
            )
        elif update.collect_audio and update.state != VADState.STARTING:
            await self._append_turn_audio(frame)
        if (
            self.state.active_response_id
            and update.state in {VADState.SPEAKING, VADState.STOPPING}
        ):
            await self._confirm_barge_in_from_vad(update)
        if update.ended:
            self._turn_stats_by_id[self.state.turn_id] = {
                "speech_duration_ms": update.speech_duration_ms,
                "total_duration_ms": update.total_duration_ms,
                "speech_frame_ratio": update.speech_frame_ratio,
            }
            VAD_ENDPOINT_DELAY.labels(self.vad.name).observe(
                self.settings.vad_end_silence_ms / 1000
            )
            await self.emit(
                EventType.VAD_SPEECH_ENDED,
                "vad",
                payload={
                    "provider": self.vad.name,
                    "state": update.state.value,
                    "speech_duration_ms": update.speech_duration_ms,
                    "total_duration_ms": update.total_duration_ms,
                    "speech_frame_ratio": update.speech_frame_ratio,
                },
            )
            await self._finalize_turn()

    async def _handle_barge_in_candidate(self, message: dict[str, Any]) -> None:
        candidate = BargeInCandidate(
            barge_in_id=str(message.get("barge_in_id") or uuid4()),
            call_id=self.call_id,
            turn_id=str(message.get("turn_id") or ""),
            response_id=str(message.get("response_id") or ""),
            detected_at_monotonic_ms=float(
                message.get("detected_at_monotonic_ms") or time.monotonic() * 1000
            ),
            last_played_sequence=int(message.get("last_played_sequence") or 0),
            played_audio_ms=float(message.get("played_audio_ms") or 0.0),
            source="browser",
        )
        with tracer.start_as_current_span("barge_in.candidate") as span:
            set_span_attributes(
                span,
                call_id=self.call_id,
                turn_id=candidate.turn_id,
                response_id=candidate.response_id,
                barge_in_id=candidate.barge_in_id,
                source=candidate.source,
                played_audio_ms=candidate.played_audio_ms,
                last_played_sequence=candidate.last_played_sequence,
            )
            if candidate.response_id != self._barge_in.active_response_id:
                self._restore_recent_response_for_candidate(candidate)
            transition = self._barge_in.candidate(candidate)
            set_span_attributes(
                span,
                barge_in_state=transition.state.value,
                reason_code=transition.reason_code,
                duplicate=transition.duplicate,
            )
        if transition.reason_code == "stale_response":
            await self._emit_barge_rejected(transition, "stale_response")
            return
        if transition.duplicate:
            return
        self._barge_in.playback_progress(
            turn_id=candidate.turn_id,
            response_id=candidate.response_id,
            last_played_sequence=candidate.last_played_sequence,
            played_audio_ms=candidate.played_audio_ms,
        )
        BARGE_IN_CANDIDATES_TOTAL.labels(candidate.source).inc()
        BARGE_IN_PLAYBACK_STOP_LATENCY.observe(0.001)
        await self.emit(
            EventType.USER_BARGE_IN_CANDIDATE,
            "barge_in",
            payload={
                "barge_in_id": candidate.barge_in_id,
                "source": candidate.source,
                "response_id": candidate.response_id,
                "last_played_sequence": candidate.last_played_sequence,
                "played_audio_ms": candidate.played_audio_ms,
            },
        )
        await self.emit(
            EventType.PIPELINE_PLAYBACK_STOPPED,
            "transport",
            payload={
                "barge_in_id": candidate.barge_in_id,
                "response_id": candidate.response_id,
                "reason_code": "speculative_candidate",
            },
        )
        await self._send_pipeline_state()

    async def _handle_playback_progress(self, message: dict[str, Any]) -> None:
        response_id = str(message.get("response_id") or "")
        cursor = self._barge_in.playback_progress(
            turn_id=str(message.get("turn_id") or ""),
            response_id=response_id,
            last_played_sequence=int(message.get("last_played_sequence") or 0),
            played_audio_ms=float(message.get("played_audio_ms") or 0.0),
        )
        if response_id in self._responses.cancelled:
            AUDIO_PLAYED_AFTER_CANCEL_MS.inc(cursor.played_audio_ms)

    async def _handle_playback_done(self, message: dict[str, Any]) -> None:
        response_id = str(message.get("response_id") or "")
        if not response_id:
            return
        self._playback_done_response_ids.add(response_id)
        self._barge_in.playback_progress(
            turn_id=str(message.get("turn_id") or ""),
            response_id=response_id,
            last_played_sequence=int(message.get("last_played_sequence") or 0),
            played_audio_ms=float(message.get("played_audio_ms") or 0.0),
        )
        self._finish_response_if_playback_done(response_id)

    async def _maybe_reject_barge_in_candidate(self, reason_code: str) -> None:
        transition = self._barge_in.reject(reason_code) or self._barge_in.reject_expired()
        if transition:
            await self._emit_barge_rejected(transition, transition.reason_code)

    async def _emit_barge_rejected(
        self, transition: Any, reason_code: str
    ) -> None:
        BARGE_IN_REJECTED_TOTAL.labels(reason_code).inc()
        await self.emit(
            EventType.USER_BARGE_IN_REJECTED,
            "barge_in",
            payload={
                "barge_in_id": transition.barge_in_id,
                "response_id": transition.response_id,
                "reason_code": reason_code,
            },
        )

    async def _confirm_barge_in_from_vad(self, update: TurnUpdate) -> None:
        response_id = self.state.active_response_id
        if not response_id:
            return
        has_client_candidate = self._barge_in.current_candidate is not None
        confirmation_ms = (
            self.settings.barge_in_confirmation_ms
            if has_client_candidate
            else self.settings.barge_in_backend_confirmation_ms
        )
        min_speech_ratio = (
            self.settings.barge_in_min_speech_ratio
            if has_client_candidate
            else self.settings.barge_in_backend_min_speech_ratio
        )
        if update.speech_duration_ms < confirmation_ms:
            return
        if update.speech_frame_ratio < min_speech_ratio:
            return
        if not has_client_candidate and self._is_in_backend_barge_in_echo_grace(response_id):
            return
        if not self._barge_in.current_candidate:
            self._barge_in.backend_candidate(turn_id=self.state.turn_id, response_id=response_id)
            BARGE_IN_CANDIDATES_TOTAL.labels("backend_vad").inc()
        started = time.perf_counter()
        transition = self._barge_in.confirm("sustained_speech")
        if not transition:
            return
        with tracer.start_as_current_span("barge_in.confirmation") as span:
            set_span_attributes(
                span,
                call_id=self.call_id,
                turn_id=transition.turn_id,
                response_id=response_id,
                reason_code=transition.reason_code,
                speech_duration_ms=update.speech_duration_ms,
                speech_frame_ratio=update.speech_frame_ratio,
                client_candidate=has_client_candidate,
            )
        BARGE_IN_CONFIRMED_TOTAL.labels(transition.reason_code).inc()
        BARGE_IN_CONFIRMATION_LATENCY.labels(transition.reason_code).observe(
            max(0.0, (time.perf_counter() - started))
        )
        await self.emit(
            EventType.USER_BARGE_IN_CONFIRMED,
            "barge_in",
            payload={
                "barge_in_id": transition.barge_in_id,
                "response_id": response_id,
                "reason_code": transition.reason_code,
                "speech_duration_ms": update.speech_duration_ms,
                "speech_frame_ratio": update.speech_frame_ratio,
            },
        )
        await self._cancel_response(response_id, "barge_in_confirmed")

    def _is_in_backend_barge_in_echo_grace(self, response_id: str) -> bool:
        grace_seconds = self.settings.barge_in_backend_echo_grace_ms / 1000
        if grace_seconds <= 0:
            return False
        now = time.monotonic()
        first_audio_at = self._response_first_audio_at.get(response_id)
        if first_audio_at is not None and now - first_audio_at < grace_seconds:
            return True
        last_audio_sent_at = self._response_last_audio_sent_at.get(response_id)
        return last_audio_sent_at is not None and now - last_audio_sent_at < grace_seconds

    def _restore_recent_response_for_candidate(self, candidate: BargeInCandidate) -> bool:
        if candidate.response_id in self._responses.cancelled:
            return False
        response_turn_id = self._responses.response_turns.get(candidate.response_id)
        if not response_turn_id:
            return False
        last_audio_sent_at = self._response_last_audio_sent_at.get(candidate.response_id)
        if last_audio_sent_at is None:
            return False
        audible_grace_seconds = max(
            self.settings.barge_in_recent_response_grace_ms / 1000,
            self._response_estimated_ms_by_id.get(candidate.response_id, 0.0) / 1000 + 1.0,
        )
        if time.monotonic() - last_audio_sent_at > audible_grace_seconds:
            return False
        turn_id = candidate.turn_id or response_turn_id
        self._responses.active_response_id = candidate.response_id
        self.state.active_response_id = candidate.response_id
        self._playback_done_response_ids.discard(candidate.response_id)
        self._barge_in.assistant_playing(
            turn_id=turn_id,
            response_id=candidate.response_id,
        )
        return True

    async def _append_turn_audio(self, frame: AudioFrame) -> None:
        self._turn_audio_bytes += len(frame.data)
        if self._turn_audio_bytes > self.settings.websocket_max_audio_bytes:
            raise ValueError("Audio turn exceeded maximum size")
        if not self._stt_session:
            raise RuntimeError("Streaming STT session is not available")
        self._turn_has_audio = True
        await self._stt_session.append_audio(frame.data, frame.sample_rate)

    async def _finalize_turn(self) -> None:
        if not self._turn_has_audio or self._turn_lock.locked():
            return
        turn_id = self.state.turn_id
        self._turn_stats_by_id.setdefault(turn_id, self._current_turn_stats())
        self._turn_has_audio = False
        self._turn_audio_bytes = 0
        await self._process_turn(turn_id)

    async def _process_turn(self, turn_id: str) -> None:
        async with self._turn_lock:
            self.state.turn_id = turn_id
            transcript = await self._run_stt(turn_id)
            ignored_reason = self._noise_turn_reason(turn_id, transcript)
            if ignored_reason:
                await self._resolve_barge_in_semantics(turn_id, transcript, forced_noise=True)
                self._last_interruption = None
                await self._ignore_noise_turn(turn_id, ignored_reason)
                return
            semantic = await self._resolve_barge_in_semantics(turn_id, transcript)
            if semantic is None:
                semantic = self._semantic_for_short_turn(transcript)
            if (
                semantic == BargeInSemantic.BACKCHANNEL
                and self.settings.barge_in_backchannel_policy != "high"
            ):
                self._conversation.append(
                    {
                        "role": "user",
                        "turn_id": turn_id,
                        "content": transcript,
                        "semantic": semantic.value,
                        "handled_as": "backchannel_ignored",
                    }
                )
                self._last_interruption = None
                return
            self._record_turn_outcome(turn_id, "accepted")
            self.state.transcript = transcript
            await self.transport.send_json(
                "transcript.final", call_id=self.call_id, turn_id=turn_id, text=transcript
            )
            self._conversation.append(
                {
                    "role": "user",
                    "turn_id": turn_id,
                    "content": transcript,
                    "semantic": semantic.value if semantic else None,
                }
            )
            await self._run_llm_tts_transport(transcript, semantic)
            if semantic:
                self._last_interruption = None

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
        STT_TURNS_COMMITTED_TOTAL.labels(self.vad.name).inc()
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

    def _record_vad_metrics(
        self,
        result: VADResult,
        previous_state: VADState,
        state: VADState,
    ) -> None:
        VAD_FRAMES_TOTAL.labels(result.provider, "speech" if result.speech else "silence").inc()
        if result.energy is not None:
            VAD_ENERGY.labels(result.provider).set(result.energy)
        if result.noise_floor is not None:
            VAD_NOISE_FLOOR.labels(result.provider).set(result.noise_floor)
        if previous_state != state:
            VAD_STATE_TRANSITIONS_TOTAL.labels(
                result.provider,
                previous_state.value,
                state.value,
            ).inc()

    def _update_vad_state(self, result: VADResult, update: TurnUpdate) -> None:
        self.state.vad = {
            "provider": result.provider,
            "state": update.state.value,
            "decision": "speech" if result.speech else "silence",
            "energy": result.energy,
            "noise_floor": result.noise_floor,
            "probability": result.probability,
            "sample_rate": result.sample_rate,
            "frame_duration_ms": result.frame_duration_ms,
            "speech_duration_ms": update.speech_duration_ms,
            "total_duration_ms": update.total_duration_ms,
            "speech_frame_ratio": update.speech_frame_ratio,
        }

    def _current_turn_stats(self) -> dict[str, float]:
        snapshot = self.turn_detector.snapshot()
        return {
            "speech_duration_ms": float(snapshot.get("speech_duration_ms", 0.0)),
            "total_duration_ms": float(snapshot.get("total_duration_ms", 0.0)),
            "speech_frame_ratio": float(snapshot.get("speech_frame_ratio", 0.0)),
        }

    def _noise_turn_reason(self, turn_id: str, transcript: str) -> str | None:
        stats = self._turn_stats_by_id.get(turn_id) or self._current_turn_stats()
        speech_duration_ms = stats.get("speech_duration_ms", 0.0)
        speech_frame_ratio = stats.get("speech_frame_ratio", 0.0)
        cleaned_transcript = transcript.strip()
        normalized_transcript = " ".join(
            cleaned_transcript.lower().strip(" \t\n\r.,!?").split()
        )
        if speech_duration_ms < self.settings.vad_min_turn_audio_ms:
            return "too_short"
        if not cleaned_transcript:
            return "empty_transcript"
        if (
            normalized_transcript in {"hi", "hello", "hey"}
            and speech_duration_ms >= 500
        ):
            return None
        if speech_frame_ratio < self.settings.vad_min_speech_frame_ratio:
            if speech_duration_ms >= self.settings.vad_min_transcribed_turn_audio_ms:
                return None
            return "low_speech_ratio"
        return None

    def _semantic_for_short_turn(self, transcript: str) -> BargeInSemantic | None:
        semantic = classify_interruption(transcript)
        if semantic == BargeInSemantic.BACKCHANNEL:
            return semantic
        return None

    def _record_turn_outcome(self, turn_id: str, outcome: str) -> None:
        stats = self._turn_stats_by_id.pop(turn_id, self._current_turn_stats())
        total_duration_ms = max(stats.get("total_duration_ms", 0.0), 0.0)
        VAD_TURN_DURATION.labels(self.vad.name, outcome).observe(total_duration_ms / 1000)

    async def _ignore_noise_turn(self, turn_id: str, reason_code: str) -> None:
        stats = self._turn_stats_by_id.get(turn_id, self._current_turn_stats())
        self._record_turn_outcome(turn_id, "ignored")
        VAD_NOISE_TURNS_IGNORED_TOTAL.labels(self.vad.name, reason_code).inc()
        await self.emit(
            EventType.VAD_NOISE_TURN_IGNORED,
            "vad",
            turn_id=turn_id,
            payload={
                "provider": self.vad.name,
                "reason_code": reason_code,
                "speech_duration_ms": stats.get("speech_duration_ms", 0.0),
                "total_duration_ms": stats.get("total_duration_ms", 0.0),
                "speech_frame_ratio": stats.get("speech_frame_ratio", 0.0),
            },
        )
        await self.transport.send_json(
            "vad.noise_turn_ignored",
            call_id=self.call_id,
            turn_id=turn_id,
            reason_code=reason_code,
        )

    async def _resolve_barge_in_semantics(
        self,
        turn_id: str,
        transcript: str,
        *,
        forced_noise: bool = False,
    ) -> BargeInSemantic | None:
        if not self._last_interruption and not forced_noise:
            return None
        started = time.perf_counter()
        self._barge_in.resolving()
        semantic = (
            BargeInSemantic.NOISE_OR_ECHO
            if forced_noise
            else classify_interruption(transcript)
        )
        BARGE_IN_CLASSIFICATION_LATENCY.labels(semantic.value).observe(
            time.perf_counter() - started
        )
        with tracer.start_as_current_span("barge_in.semantic_resolution") as span:
            set_span_attributes(
                span,
                call_id=self.call_id,
                turn_id=turn_id,
                semantic=semantic.value,
                transcript_chars=len(transcript),
                interrupted_response_id=self._last_interruption.get("response_id")
                if self._last_interruption
                else None,
            )
        await self.emit(
            EventType.USER_BARGE_IN_CLASSIFIED,
            "barge_in",
            turn_id=turn_id,
            payload={
                "semantic": semantic.value,
                "interrupted_response_id": self._last_interruption.get("response_id")
                if self._last_interruption
                else None,
            },
        )
        self._barge_in.resolved(semantic)
        return semantic

    def _llm_context(self, semantic: BargeInSemantic | None) -> dict[str, Any]:
        return {
            "call_id": self.call_id,
            "messages": self._conversation[-10:],
            "barge_in": {
                "semantic": semantic.value if semantic else None,
                "interruption": self._last_interruption,
                "instruction": self._semantic_instruction(semantic),
            }
            if semantic
            else None,
        }

    def _semantic_instruction(self, semantic: BargeInSemantic | None) -> str | None:
        if semantic == BargeInSemantic.CORRECTION:
            return "The latest user turn corrects or replaces the prior intent."
        if semantic == BargeInSemantic.CANCELLATION_REQUEST:
            return (
                "The latest user turn may request cancellation. Do not assume a durable "
                "business action is cancelled unless a tool route explicitly does it."
            )
        if semantic == BargeInSemantic.ADDITIVE_CONTEXT:
            return "The latest user turn adds context to the previous conversation."
        if semantic == BargeInSemantic.CLARIFICATION_OR_REPEAT:
            return "The user is asking for clarification or repetition."
        if semantic == BargeInSemantic.UNKNOWN_INTERRUPTION:
            return "Use the conversation and interruption metadata to infer the best response."
        return None

    async def _run_llm_tts_transport(
        self, transcript: str, semantic: BargeInSemantic | None = None
    ) -> None:
        response_id = self._responses.start_response(self.state.turn_id)
        self.state.active_response_id = response_id
        self._barge_in.assistant_playing(turn_id=self.state.turn_id, response_id=response_id)
        self._response_text_by_id[response_id] = ""
        self._response_estimated_ms_by_id[response_id] = 0.0
        self._response_audio_sequences[response_id] = 0
        self._response_first_audio_at.pop(response_id, None)
        self._response_last_audio_sent_at.pop(response_id, None)
        self._response_transport_complete.discard(response_id)
        self._playback_done_response_ids.discard(response_id)
        token_queue: FlowControlledQueue[TextQueueItem] = FlowControlledQueue(
            call_id=self.call_id,
            stage="llm_to_tts",
            depth_unit=DepthUnit.SPEAK_AHEAD_MS,
            high_watermark=self.settings.llm_to_tts_high_watermark_speak_ahead_ms,
            low_watermark=self.settings.llm_to_tts_low_watermark_speak_ahead_ms,
            hard_limit=self.settings.llm_to_tts_hard_limit_speak_ahead_ms,
            on_state_change=self._on_backpressure,
            weight_fn=lambda item: item.estimated_speech_ms
            if isinstance(item, TextChunk)
            else 0.0,
            max_items=self.settings.flow_queue_max_items,
            hard_limit_policy=self.settings.backpressure_hard_limit_policy,
        )
        audio_queue: FlowControlledQueue[AudioQueueItem] = FlowControlledQueue(
            call_id=self.call_id,
            stage="tts_to_transport",
            depth_unit=DepthUnit.AUDIO_MS,
            high_watermark=self.settings.tts_to_transport_high_watermark_audio_ms,
            low_watermark=self.settings.tts_to_transport_low_watermark_audio_ms,
            hard_limit=self.settings.tts_to_transport_hard_limit_audio_ms,
            on_state_change=self._on_backpressure,
            weight_fn=lambda item: item.duration_ms if isinstance(item, AudioChunk) else 0.0,
            max_items=self.settings.flow_queue_max_items,
            hard_limit_policy=self.settings.backpressure_hard_limit_policy,
        )
        self._live_queues = {
            "llm_to_tts": token_queue,
            "tts_to_transport": audio_queue,
        }
        llm_task = asyncio.create_task(
            self._produce_llm(transcript, token_queue, response_id, semantic)
        )
        tts_task = asyncio.create_task(self._consume_tts(token_queue, audio_queue, response_id))
        transport_task = asyncio.create_task(self._consume_transport(audio_queue, response_id))
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
        finally:
            await token_queue.close()
            await audio_queue.close()
            self._live_queues.clear()

    async def _produce_llm(
        self,
        transcript: str,
        queue: FlowControlledQueue[TextQueueItem],
        response_id: str,
        semantic: BargeInSemantic | None,
    ) -> None:
        self.state.current_stage = "llm"
        await self.emit(EventType.LLM_STARTED, "llm")
        started = time.perf_counter()
        first = True
        response_parts: list[str] = []
        sequence = 0
        turn_id = self.state.turn_id
        try:
            with tracer.start_as_current_span("pipeline.llm") as span:
                set_span_attributes(
                    span,
                    call_id=self.call_id,
                    turn_id=turn_id,
                    response_id=response_id,
                    stage="llm",
                    provider=self.llm.name,
                    model=self.llm.model,
                    transcript_chars=len(transcript),
                    barge_in_semantic=semantic.value if semantic else None,
                )
                context = self._llm_context(semantic)
                async for token in generate_tokens(self.llm, transcript, context):
                    if first:
                        first = False
                        first_token_latency = (time.perf_counter() - started) * 1000
                        LLM_FIRST_TOKEN_LATENCY.labels(
                            self.llm.name, self.llm.model
                        ).observe(first_token_latency)
                        await self.emit(EventType.LLM_FIRST_TOKEN, "llm")
                    response_parts.append(token)
                    self._response_text_by_id[response_id] = "".join(response_parts)
                    self._response_estimated_ms_by_id[response_id] = self._estimate_speech_ms(
                        self._response_text_by_id[response_id]
                    )
                    if self._is_stale_response(turn_id, response_id):
                        await self._drop_stale_chunk(
                            "llm_to_tts",
                            "text",
                            self._stale_reason(turn_id, response_id),
                            response_id=response_id,
                        )
                        break
                    sequence += 1
                    await queue.wait_if_corked()
                    await queue.put(
                        TextChunk(
                            call_id=self.call_id,
                            turn_id=turn_id,
                            response_id=response_id,
                            sequence=sequence,
                            text=token,
                            estimated_speech_ms=self._estimate_speech_ms(token),
                        )
                    )
                    self._set_queue_depth("llm_to_tts", queue.depth_weight)
                    await self._send_pipeline_state()
                    await self.transport.send_json(
                        "llm.token",
                        call_id=self.call_id,
                        turn_id=turn_id,
                        response_id=response_id,
                        sequence=sequence,
                        text=token,
                    )
                set_span_attributes(
                    span,
                    response_chars=sum(len(part) for part in response_parts),
                    max_queue_depth=max(self.state.queue_depths.get("llm_to_tts", 0), 0),
                )
        except Exception as exc:
            await self._provider_failed("llm", self.llm.name, exc)
            raise
        finally:
            await queue.put(
                EndOfStream(
                    call_id=self.call_id,
                    turn_id=turn_id,
                    response_id=response_id,
                ),
                bypass_cork=True,
            )
        response = "".join(response_parts)
        self._response_text_by_id[response_id] = response
        self._response_estimated_ms_by_id[response_id] = self._estimate_speech_ms(response)
        self.state.response = response
        if response_id not in self._interrupted_response_ids:
            self._conversation.append(
                {
                    "role": "assistant",
                    "response_id": response_id,
                    "turn_id": turn_id,
                    "status": "completed",
                    "content": response,
                }
            )
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
        token_queue: FlowControlledQueue[TextQueueItem],
        audio_queue: FlowControlledQueue[AudioQueueItem],
        response_id: str,
    ) -> None:
        self.state.current_stage = "tts"
        response_turn_id = self._responses.response_turns.get(response_id, self.state.turn_id)
        buffer = ""
        first_audio = True
        started = time.perf_counter()
        await self.emit(EventType.TTS_STARTED, "tts")
        try:
            while True:
                item = await token_queue.get()
                token_queue.task_done()
                self._set_queue_depth("llm_to_tts", token_queue.depth_weight)
                await self._send_pipeline_state()
                if isinstance(item, EndOfStream):
                    if buffer.strip():
                        first_audio = await self._synthesize_phrase(
                            buffer, audio_queue, first_audio, item.turn_id, response_id
                        )
                    break
                if self._is_stale_chunk(item):
                    await self._drop_stale_chunk(
                        "tts",
                        "text",
                        self._stale_reason(item.turn_id, item.response_id),
                        response_id=item.response_id,
                    )
                    continue
                buffer += item.text
                if len(buffer) >= 120 or any(buffer.rstrip().endswith(mark) for mark in ".?!"):
                    first_audio = await self._synthesize_phrase(
                        buffer, audio_queue, first_audio, item.turn_id, response_id
                    )
                    buffer = ""
        except QueueClosed:
            return
        except Exception as exc:
            await self._provider_failed("tts", self.tts.name, exc)
            raise
        finally:
            await audio_queue.put(
                EndOfStream(
                    call_id=self.call_id,
                    turn_id=response_turn_id,
                    response_id=response_id,
                ),
                bypass_cork=True,
            )
        latency = (time.perf_counter() - started) * 1000
        STAGE_LATENCY.labels("tts", self.tts.name).observe(latency)
        await self.emit(
            EventType.TTS_COMPLETED,
            "tts",
            payload={
                "latency_ms": latency,
                "queue_depth": audio_queue.depth_weight,
                "depth_unit": audio_queue.depth_unit,
            },
        )

    async def _synthesize_phrase(
        self,
        phrase: str,
        audio_queue: FlowControlledQueue[AudioQueueItem],
        first_audio: bool,
        turn_id: str,
        response_id: str,
    ) -> bool:
        if self._is_stale_response(turn_id, response_id):
            await self._drop_stale_chunk(
                "tts",
                "text",
                self._stale_reason(turn_id, response_id),
                response_id=response_id,
            )
            return first_audio
        phrase_started = time.perf_counter()
        sequence = self._response_audio_sequences.get(response_id, 0)
        with tracer.start_as_current_span("pipeline.tts") as span:
            set_span_attributes(
                span,
                call_id=self.call_id,
                turn_id=turn_id,
                response_id=response_id,
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
                    self._response_first_audio_at.setdefault(response_id, time.monotonic())
                    first_audio_latency = (time.perf_counter() - phrase_started) * 1000
                    TTS_FIRST_AUDIO_LATENCY.labels(
                        self.tts.name, self.tts.model
                    ).observe(first_audio_latency)
                    await self.emit(EventType.TTS_FIRST_AUDIO, "tts")
                duration_ms = self._audio_duration_ms(audio)
                if self._is_stale_response(turn_id, response_id):
                    await self._drop_stale_chunk(
                        "tts_to_transport",
                        "audio",
                        self._stale_reason(turn_id, response_id),
                        audio_ms=duration_ms,
                        response_id=response_id,
                    )
                    continue
                sequence += 1
                self._response_audio_sequences[response_id] = sequence
                await audio_queue.wait_if_corked()
                await audio_queue.put(
                    AudioChunk(
                        call_id=self.call_id,
                        turn_id=turn_id,
                        response_id=response_id,
                        sequence=sequence,
                        data=audio,
                        sample_rate=self.settings.tts_output_sample_rate,
                        duration_ms=duration_ms,
                    )
                )
                audio_bytes += len(audio)
                chunks += 1
                self._set_queue_depth("tts_to_transport", audio_queue.depth_weight)
                await self._send_pipeline_state()
            set_span_attributes(
                span,
                audio_bytes=audio_bytes,
                audio_chunks=chunks,
                queue_depth=audio_queue.depth_weight,
                queue_depth_unit=audio_queue.depth_unit,
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
        self,
        audio_queue: FlowControlledQueue[AudioQueueItem],
        response_id: str,
    ) -> None:
        self.state.current_stage = "transport"
        bytes_sent = 0
        chunks_sent = 0
        while True:
            try:
                item = await audio_queue.get()
            except QueueClosed:
                return
            audio_queue.task_done()
            self._set_queue_depth("tts_to_transport", audio_queue.depth_weight)
            await self._send_pipeline_state()
            if isinstance(item, EndOfStream):
                self._response_transport_complete.add(response_id)
                self._finish_response_if_playback_done(response_id)
                await self.emit(
                    EventType.TRANSPORT_AUDIO_SENT,
                    "transport",
                    payload={"bytes": bytes_sent, "chunks": chunks_sent, "queue_depth": 0},
                )
                return
            if self._is_stale_chunk(item) or item.response_id != response_id:
                await self._drop_stale_chunk(
                    "transport",
                    "audio",
                    self._stale_reason(item.turn_id, item.response_id),
                    audio_ms=item.duration_ms,
                    response_id=item.response_id,
                )
                continue
            await self.transport.send_audio(
                item.data,
                call_id=item.call_id,
                turn_id=item.turn_id,
                response_id=item.response_id,
                sequence=item.sequence,
                sample_rate=item.sample_rate,
            )
            self._response_first_audio_at.setdefault(response_id, time.monotonic())
            self._response_last_audio_sent_at[response_id] = time.monotonic()
            bytes_sent += len(item.data)
            chunks_sent += 1

    def _finish_response_if_playback_done(self, response_id: str) -> None:
        if response_id not in self._response_transport_complete:
            return
        if response_id not in self._playback_done_response_ids:
            return
        if response_id in self._responses.cancelled:
            return
        if self._responses.finish_response(response_id):
            self.state.active_response_id = self._responses.active_response_id
            self._barge_in.assistant_finished(response_id)

    async def _on_backpressure(self, transition: BackpressureTransition) -> None:
        self.state.backpressure[transition.stage] = BackpressureStageState(
            corked=transition.corked,
            hard_limited=transition.hard_limited,
            depth=transition.depth,
            depth_unit=transition.depth_unit,
            item_count=transition.item_count,
            reason_code=transition.reason_code if transition.corked else None,
        )
        self.state.corked = any(item.corked for item in self.state.backpressure.values())
        self.state.cork_reason = transition.reason if self.state.corked else None
        event_type = (
            EventType.PIPELINE_HARD_LIMIT_REACHED
            if transition.hard_limited
            else EventType.PIPELINE_CORKED
            if transition.corked
            else EventType.PIPELINE_UNCORKED
        )
        with tracer.start_as_current_span("pipeline.backpressure") as span:
            set_span_attributes(
                span,
                call_id=self.call_id,
                turn_id=self.state.turn_id,
                stage=transition.stage,
                corked=transition.corked,
                hard_limited=transition.hard_limited,
                reason_code=transition.reason_code,
                queue_depth=transition.depth,
                queue_depth_unit=transition.depth_unit,
                queue_items=transition.item_count,
            )
            await self.emit(
                event_type,
                "backpressure",
                payload={
                    "stage": transition.stage,
                    "reason": transition.reason,
                    "reason_code": transition.reason_code,
                    "queue_depth": transition.depth,
                    "depth_unit": transition.depth_unit,
                    "item_count": transition.item_count,
                    "hard_limited": transition.hard_limited,
                },
            )
        await self.transport.send_json(
            str(event_type),
            corked=self.state.corked,
            stage=transition.stage,
            reason=transition.reason,
            reason_code=transition.reason_code,
            queue_depths=self.state.queue_depths,
            backpressure=self.state.backpressure_payload(),
        )
        if (
            transition.hard_limited
            and self.settings.backpressure_hard_limit_policy == "cancel_response"
        ):
            response_id = self.state.active_response_id
            if response_id:
                await self._cancel_response(response_id, "queue_hard_limit")

    def _set_queue_depth(self, stage: str, depth: float) -> None:
        self.state.queue_depths[stage] = depth

    def _estimate_speech_ms(self, text: str) -> float:
        return len(text) / self.settings.speech_chars_per_second * 1000

    def _audio_duration_ms(self, audio: bytes) -> float:
        return len(audio) / (2 * self.settings.tts_output_sample_rate) * 1000

    def _is_stale_response(self, turn_id: str, response_id: str) -> bool:
        return not self._responses.is_response_active(response_id, turn_id)

    def _is_stale_chunk(self, item: TextChunk | AudioChunk) -> bool:
        return self._is_stale_response(item.turn_id, item.response_id)

    def _stale_reason(self, turn_id: str, response_id: str) -> str:
        if self._responses.is_response_cancelled(response_id):
            return "response_cancelled"
        if turn_id != self.state.turn_id:
            return "stale_turn"
        return "stale_response"

    async def _drop_stale_chunk(
        self,
        stage: str,
        chunk_type: str,
        reason_code: str,
        *,
        response_id: str,
        audio_ms: float = 0,
    ) -> None:
        STALE_CHUNKS_DROPPED_TOTAL.labels(stage, chunk_type, reason_code).inc()
        if chunk_type == "audio" and audio_ms:
            STALE_AUDIO_DROPPED_MS_TOTAL.labels(stage, reason_code).inc(audio_ms)
        await self.emit(
            EventType.PIPELINE_STALE_CHUNK_DROPPED,
            stage,
            payload={
                "chunk_type": chunk_type,
                "reason_code": reason_code,
                "response_id": response_id,
                "audio_ms": audio_ms,
            },
        )

    async def _cancel_response(self, response_id: str, reason_code: str) -> None:
        started = time.perf_counter()
        self._barge_in.begin_cancelling(response_id)
        if not self._responses.cancel_response(response_id, reason_code):
            return
        self.state.active_response_id = self._responses.active_response_id
        with tracer.start_as_current_span("barge_in.cancel_response") as span:
            set_span_attributes(
                span,
                call_id=self.call_id,
                response_id=response_id,
                reason_code=reason_code,
            )
            text_queue = self._live_queues.get("llm_to_tts")
            audio_queue = self._live_queues.get("tts_to_transport")
            flushed_text = (
                await text_queue.flush(
                    lambda item: isinstance(item, TextChunk) and item.response_id == response_id
                )
                if text_queue
                else []
            )
            flushed_audio = (
                await audio_queue.flush(
                    lambda item: isinstance(item, AudioChunk) and item.response_id == response_id
                )
                if audio_queue
                else []
            )
            set_span_attributes(
                span,
                flushed_text_items=len(flushed_text),
                flushed_audio_items=len(flushed_audio),
            )
        stale_audio_ms = sum(
            item.duration_ms for item in flushed_audio if isinstance(item, AudioChunk)
        )
        if flushed_text:
            STALE_CHUNKS_DROPPED_TOTAL.labels(
                "llm_to_tts", "text", "response_cancelled"
            ).inc(len(flushed_text))
        if flushed_audio:
            STALE_CHUNKS_DROPPED_TOTAL.labels(
                "tts_to_transport", "audio", "response_cancelled"
            ).inc(len(flushed_audio))
            STALE_AUDIO_DROPPED_MS_TOTAL.labels(
                "tts_to_transport", "response_cancelled"
            ).inc(stale_audio_ms)
        self._mark_response_interrupted(response_id)
        self._barge_in.cancelled(response_id)
        BARGE_IN_BACKEND_CANCEL_LATENCY.observe(time.perf_counter() - started)
        await self.emit(
            EventType.PIPELINE_RESPONSE_CANCELLED,
            "barge_in",
            payload={
                "response_id": response_id,
                "reason_code": reason_code,
                "flushed_text_items": len(flushed_text),
                "flushed_audio_items": len(flushed_audio),
                "stale_audio_ms": stale_audio_ms,
            },
        )
        await self._safe_send_json(
            EventType.PIPELINE_RESPONSE_CANCELLED.value,
            call_id=self.call_id,
            turn_id=self._responses.response_turns.get(response_id, self.state.turn_id),
            response_id=response_id,
            reason_code=reason_code,
        )
        asyncio.create_task(self._cancel_provider_streams(response_id))

    async def _cancel_provider_streams(self, response_id: str) -> None:
        with tracer.start_as_current_span("provider.cancel") as span:
            set_span_attributes(span, call_id=self.call_id, response_id=response_id)
            with suppress(Exception):
                await self.tts.cancel(response_id)
            with suppress(Exception):
                await self.llm.cancel(response_id)

    def _mark_response_interrupted(self, response_id: str) -> None:
        if response_id in self._interrupted_response_ids:
            return
        self._interrupted_response_ids.add(response_id)
        generated_text = self._response_text_by_id.get(response_id, "")
        estimated_ms = max(self._response_estimated_ms_by_id.get(response_id, 0.0), 1.0)
        cursor = self._barge_in.playback_cursor
        played_ms = cursor.played_audio_ms if cursor and cursor.response_id == response_id else 0.0
        ratio = max(0.0, min(1.0, played_ms / estimated_ms))
        spoken_chars = int(len(generated_text) * ratio)
        spoken_text = generated_text[:spoken_chars]
        INTERRUPTED_RESPONSE_SPOKEN_RATIO.observe(ratio)
        interruption = {
            "response_id": response_id,
            "turn_id": self._responses.response_turns.get(response_id),
            "generated_text": generated_text,
            "spoken_text": spoken_text,
            "played_audio_ms": played_ms,
            "last_played_sequence": cursor.last_played_sequence if cursor else 0,
            "spoken_ratio": ratio,
        }
        self._last_interruption = interruption
        self._conversation.append(
            {
                "role": "assistant",
                "response_id": response_id,
                "turn_id": interruption["turn_id"],
                "status": "interrupted",
                "content": spoken_text,
                "generated_text": generated_text,
                "played_audio_ms": played_ms,
                "last_played_sequence": interruption["last_played_sequence"],
            }
        )

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
        await self._safe_send_json(
            "pipeline.state",
            state={
                "stage": self.state.current_stage,
                "corked": self.state.corked,
                "cork_reason": self.state.cork_reason,
                "queue_depths": self.state.queue_depths,
                "backpressure": self.state.backpressure_payload(),
                "vad": self.state.vad,
                "active_response_id": self.state.active_response_id,
                "barge_in": self._barge_in.snapshot(),
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
        await self._safe_send_json(
            "pipeline.event",
            event=event.model_dump(mode="json"),
            state={
                "stage": stage,
                "corked": self.state.corked,
                "cork_reason": self.state.cork_reason,
                "queue_depths": self.state.queue_depths,
                "backpressure": self.state.backpressure_payload(),
                "vad": self.state.vad,
                "active_response_id": self.state.active_response_id,
                "barge_in": self._barge_in.snapshot(),
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
        await self._safe_send_json("error", message=error)

    async def _safe_send_json(self, event_type: str, **payload: Any) -> None:
        with suppress(Exception):
            await self.transport.send_json(event_type, **payload)

    async def _end_call(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._failed:
            return
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
        with suppress(Exception):
            await self.temporal.signal(
                self.call_id,
                "call_completed",
                {"summary": self.state.response},
            )
        await self._safe_send_json("call.ended", call_id=self.call_id)
