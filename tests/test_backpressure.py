import asyncio

import pytest

from apps.api.pipeline.backpressure import (
    BackpressureTransition,
    DepthUnit,
    FlowControlledQueue,
)
from apps.api.pipeline.events import AudioChunk, TextChunk
from apps.api.telemetry.metrics import QUEUE_DEPTH


async def noop_transition(_transition: BackpressureTransition) -> None:
    return None


def text_chunk(
    text: str,
    *,
    turn_id: str = "turn-1",
    response_id: str = "response-1",
    sequence: int = 1,
) -> TextChunk:
    return TextChunk(
        call_id="call-1",
        turn_id=turn_id,
        response_id=response_id,
        sequence=sequence,
        text=text,
        estimated_speech_ms=len(text) / 10 * 1000,
    )


def audio_chunk(
    duration_ms: float,
    *,
    turn_id: str = "turn-1",
    response_id: str = "response-1",
    sequence: int = 1,
) -> AudioChunk:
    sample_rate = 24_000
    byte_count = int(duration_ms / 1000 * sample_rate * 2)
    return AudioChunk(
        call_id="call-1",
        turn_id=turn_id,
        response_id=response_id,
        sequence=sequence,
        data=b"0" * byte_count,
        sample_rate=sample_rate,
        duration_ms=duration_ms,
    )


@pytest.mark.asyncio
async def test_speak_ahead_queue_corks_and_uncorks_with_hysteresis() -> None:
    changes: list[tuple[str, float]] = []

    async def changed(transition: BackpressureTransition) -> None:
        changes.append((transition.transition, transition.depth))

    queue: FlowControlledQueue[TextChunk] = FlowControlledQueue(
        call_id="call-1",
        stage="llm_to_tts",
        depth_unit=DepthUnit.SPEAK_AHEAD_MS,
        high_watermark=1200,
        low_watermark=300,
        hard_limit=2500,
        weight_fn=lambda item: item.estimated_speech_ms,
        on_state_change=changed,
    )
    await queue.put(text_chunk("hello"))  # 500 ms
    await queue.put(text_chunk("world"))  # 1000 ms, below high watermark
    assert queue.corked is False

    await queue.put(text_chunk("abc"))  # 1300 ms, corks
    assert queue.corked is True
    assert changes == [("corked", 1300)]

    assert (await queue.get()).text == "hello"
    assert queue.corked is True  # 800 ms remains, above low watermark
    assert len(changes) == 1

    assert (await queue.get()).text == "world"
    assert queue.corked is False  # 300 ms remains, low watermark reached
    assert changes[-1] == ("uncorked", 300)


@pytest.mark.asyncio
async def test_audio_queue_uses_playable_audio_ms_depth() -> None:
    changes: list[BackpressureTransition] = []

    async def changed(transition: BackpressureTransition) -> None:
        changes.append(transition)

    queue: FlowControlledQueue[AudioChunk] = FlowControlledQueue(
        call_id="call-1",
        stage="tts_to_transport",
        depth_unit=DepthUnit.AUDIO_MS,
        high_watermark=1200,
        low_watermark=300,
        hard_limit=2500,
        weight_fn=lambda item: item.duration_ms,
        on_state_change=changed,
    )
    await queue.put(audio_chunk(700))
    await queue.put(audio_chunk(600))

    assert queue.depth_weight == 1300
    assert queue.depth_items == 2
    assert queue.corked is True
    assert changes[0].depth_unit == "audio_ms"


@pytest.mark.asyncio
async def test_wait_if_corked_blocks_and_resumes_on_uncork() -> None:
    queue: FlowControlledQueue[TextChunk] = FlowControlledQueue(
        call_id="call-1",
        stage="llm_to_tts",
        depth_unit=DepthUnit.SPEAK_AHEAD_MS,
        high_watermark=100,
        low_watermark=0,
        hard_limit=1000,
        weight_fn=lambda item: item.estimated_speech_ms,
        on_state_change=noop_transition,
    )
    await queue.put(text_chunk("aa"))  # 200 ms
    waiter = asyncio.create_task(queue.wait_if_corked())
    await asyncio.sleep(0)
    assert not waiter.done()

    await queue.get()
    await asyncio.wait_for(waiter, timeout=1)


@pytest.mark.asyncio
async def test_waiting_producer_unblocks_on_cancel_and_close() -> None:
    queue: FlowControlledQueue[TextChunk] = FlowControlledQueue(
        call_id="call-1",
        stage="llm_to_tts",
        depth_unit=DepthUnit.SPEAK_AHEAD_MS,
        high_watermark=100,
        low_watermark=0,
        hard_limit=1000,
        weight_fn=lambda item: item.estimated_speech_ms,
        on_state_change=noop_transition,
    )
    await queue.put(text_chunk("aa"))
    waiter = asyncio.create_task(queue.wait_if_corked())
    await asyncio.sleep(0)
    await queue.cancel()
    await asyncio.wait_for(waiter, timeout=1)

    queue = FlowControlledQueue(
        call_id="call-1",
        stage="llm_to_tts",
        depth_unit=DepthUnit.SPEAK_AHEAD_MS,
        high_watermark=100,
        low_watermark=0,
        hard_limit=1000,
        weight_fn=lambda item: item.estimated_speech_ms,
        on_state_change=noop_transition,
    )
    await queue.put(text_chunk("aa"))
    waiter = asyncio.create_task(queue.wait_if_corked())
    await asyncio.sleep(0)
    await queue.close()
    await asyncio.wait_for(waiter, timeout=1)


@pytest.mark.asyncio
async def test_flush_removes_only_matching_response_chunks() -> None:
    queue: FlowControlledQueue[TextChunk] = FlowControlledQueue(
        call_id="call-1",
        stage="llm_to_tts",
        depth_unit=DepthUnit.SPEAK_AHEAD_MS,
        high_watermark=2000,
        low_watermark=300,
        hard_limit=3000,
        weight_fn=lambda item: item.estimated_speech_ms,
        on_state_change=noop_transition,
    )
    await queue.put(text_chunk("old", response_id="old"))
    await queue.put(text_chunk("new", response_id="new"))

    removed = await queue.flush(lambda item: item.response_id == "old")

    assert [item.response_id for item in removed] == ["old"]
    assert queue.depth_items == 1
    assert (await queue.get()).response_id == "new"


@pytest.mark.asyncio
async def test_hard_limit_transition_is_triggered() -> None:
    transitions: list[BackpressureTransition] = []

    async def changed(transition: BackpressureTransition) -> None:
        transitions.append(transition)

    queue: FlowControlledQueue[TextChunk] = FlowControlledQueue(
        call_id="call-1",
        stage="llm_to_tts",
        depth_unit=DepthUnit.SPEAK_AHEAD_MS,
        high_watermark=1000,
        low_watermark=100,
        hard_limit=1200,
        weight_fn=lambda item: item.estimated_speech_ms,
        on_state_change=changed,
    )
    await queue.put(text_chunk("x" * 13))  # 1300 ms

    assert queue.corked is True
    assert queue.hard_limited is True
    assert transitions[0].hard_limited is True
    assert transitions[0].reason_code == "queue_hard_limit"


@pytest.mark.asyncio
async def test_on_state_change_runs_outside_internal_lock() -> None:
    queue: FlowControlledQueue[TextChunk] | None = None
    callback_entered = asyncio.Event()

    async def changed(_transition: BackpressureTransition) -> None:
        callback_entered.set()
        assert queue is not None
        await asyncio.wait_for(queue.flush(lambda _item: False), timeout=1)

    queue = FlowControlledQueue(
        call_id="call-1",
        stage="llm_to_tts",
        depth_unit=DepthUnit.SPEAK_AHEAD_MS,
        high_watermark=100,
        low_watermark=0,
        hard_limit=1000,
        weight_fn=lambda item: item.estimated_speech_ms,
        on_state_change=changed,
    )

    await queue.put(text_chunk("aa"))
    await asyncio.wait_for(callback_entered.wait(), timeout=1)


def test_metrics_use_stable_queue_labels() -> None:
    label_names = QUEUE_DEPTH._labelnames
    assert "stage" in label_names
    assert "depth_unit" in label_names
    assert "call_id" not in label_names
    assert "turn_id" not in label_names
    assert "response_id" not in label_names
