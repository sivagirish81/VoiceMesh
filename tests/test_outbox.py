from uuid import uuid4

import pytest


class MemoryTransaction:
    def __init__(self) -> None:
        self.events: set[str] = set()
        self.outbox: set[str] = set()

    async def persist_critical(self, event_id: str, idempotency_key: str) -> bool:
        if idempotency_key in self.events:
            return False
        self.events.add(idempotency_key)
        self.outbox.add(event_id)
        return True


@pytest.mark.asyncio
async def test_outbox_event_is_persisted_once() -> None:
    transaction = MemoryTransaction()
    event_id = str(uuid4())
    assert await transaction.persist_critical(event_id, "call:started") is True
    assert await transaction.persist_critical(event_id, "call:started") is False
    assert transaction.outbox == {event_id}

