import asyncio

import pytest

from apps.api.pipeline.backpressure import BackpressureController


@pytest.mark.asyncio
async def test_backpressure_corks_and_uncorks() -> None:
    changes: list[tuple[bool, int]] = []

    async def changed(corked: bool, reason: str, depth: int) -> None:
        changes.append((corked, depth))

    queue: BackpressureController[str] = BackpressureController(
        call_id="call-1",
        stage="llm_to_tts",
        high_watermark=3,
        low_watermark=1,
        on_state_change=changed,
    )
    await queue.put("one")
    await queue.put("two")
    await queue.put("three")
    assert queue.corked is True
    assert changes == [(True, 3)]

    assert await queue.get() == "one"
    assert await queue.get() == "two"
    assert queue.corked is False
    assert changes[-1] == (False, 1)


@pytest.mark.asyncio
async def test_critical_items_are_not_dropped_when_queue_is_full() -> None:
    async def changed(corked: bool, reason: str, depth: int) -> None:
        return None

    queue: BackpressureController[str] = BackpressureController(
        call_id="call-1",
        stage="critical",
        high_watermark=2,
        low_watermark=0,
        on_state_change=changed,
    )
    await queue.put("token")
    await queue.put("final transcript")
    await queue.put("final llm response")
    pending = asyncio.create_task(queue.put("next"))
    await asyncio.sleep(0)
    assert not pending.done()
    assert [await queue.get(), await queue.get(), await queue.get()] == [
        "token",
        "final transcript",
        "final llm response",
    ]
    await pending
    assert await queue.get() == "next"

