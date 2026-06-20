import asyncio
import logging
import time
from collections.abc import Sequence

from apps.api.analytics.clickhouse.client import ClickHouseCloudClient
from apps.api.analytics.clickhouse.models import AnalyticsEventRow, row_values
from apps.api.telemetry.metrics import (
    CLICKHOUSE_BATCH_SIZE,
    CLICKHOUSE_BUFFER_ROWS,
    CLICKHOUSE_INSERT_BATCHES_TOTAL,
    CLICKHOUSE_INSERT_FAILURES_TOTAL,
    CLICKHOUSE_INSERT_LATENCY,
    CLICKHOUSE_INSERT_ROWS_TOTAL,
    CLICKHOUSE_WRITER_UP,
)

logger = logging.getLogger(__name__)


class ClickHouseBatchWriter:
    def __init__(
        self,
        client: ClickHouseCloudClient,
        *,
        max_rows: int = 500,
        flush_seconds: float = 1.0,
    ) -> None:
        self._client = client
        self._max_rows = max_rows
        self._flush_seconds = flush_seconds
        self._rows: list[AnalyticsEventRow] = []
        self._first_buffered_at: float | None = None
        self._lock = asyncio.Lock()

    @property
    def buffered_rows(self) -> int:
        return len(self._rows)

    async def add(self, row: AnalyticsEventRow) -> None:
        should_flush = False
        async with self._lock:
            if not self._rows:
                self._first_buffered_at = time.monotonic()
            self._rows.append(row)
            CLICKHOUSE_BUFFER_ROWS.set(len(self._rows))
            should_flush = len(self._rows) >= self._max_rows
        if should_flush:
            await self.flush()

    async def flush_if_due(self) -> None:
        async with self._lock:
            first_buffered_at = self._first_buffered_at
            due = bool(
                self._rows
                and first_buffered_at is not None
                and time.monotonic() - first_buffered_at >= self._flush_seconds
            )
        if due:
            await self.flush()

    async def flush(self) -> int:
        async with self._lock:
            rows = self._rows
            self._rows = []
            self._first_buffered_at = None
            CLICKHOUSE_BUFFER_ROWS.set(0)
        if not rows:
            return 0
        await self.insert_rows(rows)
        return len(rows)

    async def insert_rows(self, rows: Sequence[AnalyticsEventRow]) -> None:
        started = time.perf_counter()
        try:
            values = [row_values(row) for row in rows]
            await self._client.insert_voice_events(values)
            CLICKHOUSE_INSERT_BATCHES_TOTAL.labels("success").inc()
            CLICKHOUSE_INSERT_ROWS_TOTAL.labels("success").inc(len(rows))
            CLICKHOUSE_BATCH_SIZE.observe(len(rows))
            CLICKHOUSE_WRITER_UP.set(1)
        except Exception as exc:
            reason_code = _stable_failure_reason(exc)
            CLICKHOUSE_INSERT_BATCHES_TOTAL.labels("failed").inc()
            CLICKHOUSE_INSERT_ROWS_TOTAL.labels("failed").inc(len(rows))
            CLICKHOUSE_INSERT_FAILURES_TOTAL.labels(reason_code).inc()
            CLICKHOUSE_WRITER_UP.set(0)
            logger.exception("ClickHouse insert failed", extra={"reason_code": reason_code})
            raise
        finally:
            CLICKHOUSE_INSERT_LATENCY.observe(time.perf_counter() - started)

    async def close(self) -> None:
        await self.flush()


def _stable_failure_reason(exc: Exception) -> str:
    name = exc.__class__.__name__.lower()
    if "timeout" in name:
        return "timeout"
    if "auth" in name or "permission" in name:
        return "auth_failed"
    if "connect" in name or "network" in name:
        return "network_error"
    return "insert_failed"
