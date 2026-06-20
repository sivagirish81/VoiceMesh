import asyncio
from datetime import UTC, datetime

from apps.api.analytics.clickhouse.models import AnalyticsEventRow
from apps.api.analytics.clickhouse.writer import ClickHouseBatchWriter


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.inserted: list[list[list[object]]] = []

    async def insert_voice_events(self, rows: list[list[object]]) -> None:
        self.inserted.append(rows)


def row(event_id: str) -> AnalyticsEventRow:
    return AnalyticsEventRow(
        event_id=event_id,
        event_type="call.started",
        event_version=1,
        event_time=datetime.now(UTC),
        tenant_id="tenant",
        assistant_id="assistant",
        call_id="call",
        turn_id="turn",
        response_id="",
        sequence=1,
        stage="call",
        provider="",
        model="",
        status="started",
        reason_code="",
        latency_ms=None,
        duration_ms=None,
        quantity=None,
        unit="",
        trace_id="",
        idempotency_key=event_id,
        payload_json="{}",
    )


async def test_writer_flushes_at_row_threshold() -> None:
    client = FakeClickHouseClient()
    writer = ClickHouseBatchWriter(client, max_rows=2, flush_seconds=60)  # type: ignore[arg-type]

    await writer.add(row("event-1"))
    assert client.inserted == []

    await writer.add(row("event-2"))

    assert len(client.inserted) == 1
    assert len(client.inserted[0]) == 2


async def test_writer_flush_if_due() -> None:
    client = FakeClickHouseClient()
    writer = ClickHouseBatchWriter(client, max_rows=100, flush_seconds=0.001)  # type: ignore[arg-type]

    await writer.add(row("event-1"))
    await asyncio.sleep(0.002)
    await writer.flush_if_due()

    assert client.inserted


async def test_writer_does_not_insert_empty_batches() -> None:
    client = FakeClickHouseClient()
    writer = ClickHouseBatchWriter(client, max_rows=2, flush_seconds=1)  # type: ignore[arg-type]

    flushed = await writer.flush()

    assert flushed == 0
    assert client.inserted == []
