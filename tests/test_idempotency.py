from uuid import uuid4

import pytest

from apps.api.events.schemas import EventType, PipelineEvent


class MemoryIdempotencyStore:
    def __init__(self) -> None:
        self.keys: set[str] = set()
        self.transitions = 0

    async def apply(self, event: PipelineEvent) -> bool:
        if event.idempotency_key in self.keys:
            return False
        self.keys.add(event.idempotency_key)
        self.transitions += 1
        return True


@pytest.mark.asyncio
async def test_duplicate_event_is_ignored_without_state_transition() -> None:
    store = MemoryIdempotencyStore()
    event = PipelineEvent.create(
        call_id="call-1",
        turn_id="turn-1",
        event_type=EventType.CALL_STARTED,
        stage="transport",
        sequence_number=1,
        idempotency_key=f"fixed-{uuid4()}",
    )
    assert await store.apply(event) is True
    assert await store.apply(event) is False
    assert store.transitions == 1

