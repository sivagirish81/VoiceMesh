import asyncio
import time
from typing import Any

import pytest

from apps.api.config import Settings
from apps.api.events.schemas import EventType
from apps.api.pipeline.backpressure import DepthUnit, FlowControlledQueue
from apps.api.pipeline.barge_in import BargeInCoordinator
from apps.api.pipeline.events import AudioChunk, PipelineState, TextChunk
from apps.api.pipeline.stages.vad import TurnUpdate, VADState
from apps.api.pipeline.stream_module import ResponseController, StreamModule


class FailingDebugTransport:
    async def send_json(self, event_type: str, **payload: Any) -> None:
        raise RuntimeError("websocket is already closed")


class RecordingProducer:
    def __init__(self) -> None:
        self.events = []

    async def publish(self, event: Any) -> None:
        self.events.append(event)


class RecordingTemporal:
    def __init__(self) -> None:
        self.signals = []

    async def signal(self, call_id: str, signal_name: str, payload: dict[str, Any]) -> None:
        self.signals.append((call_id, signal_name, payload))


class CancellableProvider:
    def __init__(self) -> None:
        self.cancelled: list[str] = []

    async def cancel(self, response_id: str) -> None:
        self.cancelled.append(response_id)


async def noop_transition(_transition: Any) -> None:
    return None


def make_module(settings: Settings | None = None) -> StreamModule:
    module = object.__new__(StreamModule)
    module.call_id = "call-test"
    module.settings = settings or Settings()
    module.transport = FailingDebugTransport()
    module.producer = RecordingProducer()
    module.temporal = RecordingTemporal()
    module.state = PipelineState(call_id="call-test")
    module._turn_stats_by_id = {}
    module._usage_expectations = {}
    module._closed = False
    module._failed = False
    module._call_started_at = time.monotonic()
    module._responses = ResponseController()
    module._live_queues = {}
    module._barge_in = BargeInCoordinator(
        call_id="call-test",
        candidate_retention_ms=module.settings.barge_in_candidate_retention_ms,
    )
    module._conversation = []
    module._response_text_by_id = {}
    module._response_estimated_ms_by_id = {}
    module._response_audio_sequences = {}
    module._response_first_audio_at = {}
    module._response_last_audio_sent_at = {}
    module._response_transport_complete = set()
    module._playback_done_response_ids = set()
    module._interrupted_response_ids = set()
    module._last_interruption = None
    module.llm = CancellableProvider()
    module.tts = CancellableProvider()
    return module


def test_transcribed_short_greeting_is_not_rejected_only_for_low_speech_ratio() -> None:
    module = make_module(
        Settings(
            vad_min_turn_audio_ms=300,
            vad_min_speech_frame_ratio=0.60,
            vad_min_transcribed_turn_audio_ms=700,
        )
    )
    module._turn_stats_by_id["turn-1"] = {
        "speech_duration_ms": 853.0,
        "speech_frame_ratio": 0.425,
    }

    assert module._noise_turn_reason("turn-1", "Hello.") is None


def test_weak_short_transcript_is_still_rejected() -> None:
    module = make_module(
        Settings(
            vad_min_turn_audio_ms=300,
            vad_min_speech_frame_ratio=0.60,
            vad_min_transcribed_turn_audio_ms=700,
        )
    )
    module._turn_stats_by_id["turn-1"] = {
        "speech_duration_ms": 420.0,
        "speech_frame_ratio": 0.425,
    }

    assert module._noise_turn_reason("turn-1", "Hello.") == "low_speech_ratio"


@pytest.mark.asyncio
async def test_end_call_publishes_final_events_when_debug_websocket_is_closed() -> None:
    module = make_module()

    await module._end_call()

    event_types = [event.event_type for event in module.producer.events]
    assert event_types == [
        EventType.USAGE_FINALIZATION_BARRIER,
        EventType.CALL_ENDED,
    ]
    assert module.temporal.signals == [
        ("call-test", "call_completed", {"summary": ""}),
    ]


@pytest.mark.asyncio
async def test_confirmed_cancel_flushes_only_matching_response_and_skips_temporal() -> None:
    module = make_module()
    response_id = module._responses.start_response("turn-1")
    module.state.turn_id = "turn-1"
    module.state.active_response_id = response_id
    module._barge_in.assistant_playing(turn_id="turn-1", response_id=response_id)
    module._response_text_by_id[response_id] = "This is the generated answer."
    module._response_estimated_ms_by_id[response_id] = 1000
    module._barge_in.playback_progress(
        turn_id="turn-1",
        response_id=response_id,
        last_played_sequence=1,
        played_audio_ms=250,
    )
    other_response_id = "response-new"
    text_queue = FlowControlledQueue(
        call_id="call-test",
        stage="llm_to_tts",
        depth_unit=DepthUnit.SPEAK_AHEAD_MS,
        high_watermark=1200,
        low_watermark=300,
        hard_limit=2500,
        on_state_change=noop_transition,
        weight_fn=lambda item: item.estimated_speech_ms,
    )
    audio_queue = FlowControlledQueue(
        call_id="call-test",
        stage="tts_to_transport",
        depth_unit=DepthUnit.AUDIO_MS,
        high_watermark=1200,
        low_watermark=300,
        hard_limit=2500,
        on_state_change=noop_transition,
        weight_fn=lambda item: item.duration_ms,
    )
    await text_queue.put(
        TextChunk("call-test", "turn-1", response_id, 1, "old", 200)
    )
    await text_queue.put(
        TextChunk("call-test", "turn-2", other_response_id, 1, "new", 200)
    )
    await audio_queue.put(
        AudioChunk("call-test", "turn-1", response_id, 1, b"0" * 4800, 24000, 100)
    )
    module._live_queues = {
        "llm_to_tts": text_queue,
        "tts_to_transport": audio_queue,
    }

    await module._cancel_response(response_id, "barge_in_confirmed")
    await asyncio.sleep(0)

    assert module.state.active_response_id is None
    assert response_id in module._interrupted_response_ids
    assert (await text_queue.get()).response_id == other_response_id
    assert module.temporal.signals == []


@pytest.mark.asyncio
async def test_backend_barge_in_confirmation_is_suppressed_during_echo_grace() -> None:
    module = make_module(
        Settings(
            barge_in_backend_echo_grace_ms=600,
            barge_in_backend_confirmation_ms=350,
            barge_in_backend_min_speech_ratio=0.75,
        )
    )
    response_id = module._responses.start_response("turn-1")
    module.state.turn_id = "turn-1"
    module.state.active_response_id = response_id
    module._barge_in.assistant_playing(turn_id="turn-1", response_id=response_id)
    module._response_first_audio_at[response_id] = time.monotonic()

    await module._confirm_barge_in_from_vad(
        TurnUpdate(
            previous_state=VADState.STARTING,
            state=VADState.SPEAKING,
            started=True,
            speech_duration_ms=500,
            speech_frame_ratio=1.0,
        )
    )

    assert response_id not in module._interrupted_response_ids
    assert [event.event_type for event in module.producer.events] == []


@pytest.mark.asyncio
async def test_playback_done_clears_active_response_after_transport_completes() -> None:
    module = make_module()
    response_id = module._responses.start_response("turn-1")
    module.state.active_response_id = response_id
    module._barge_in.assistant_playing(turn_id="turn-1", response_id=response_id)
    module._response_transport_complete.add(response_id)

    await module._handle_playback_done(
        {
            "response_id": response_id,
            "turn_id": "turn-1",
            "last_played_sequence": 3,
            "played_audio_ms": 900,
        }
    )

    assert module.state.active_response_id is None
    assert module._barge_in.active_response_id is None
