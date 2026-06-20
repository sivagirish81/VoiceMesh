import asyncio
import logging
import random
from collections.abc import Iterable

from opentelemetry import trace
from opentelemetry.trace import SpanKind

from apps.api.analytics.clickhouse.client import ClickHouseCloudClient
from apps.api.analytics.clickhouse.normalizer import (
    AnalyticsNormalizationError,
    normalize_consumed_event,
)
from apps.api.analytics.clickhouse.writer import ClickHouseBatchWriter
from apps.api.config import Settings
from apps.api.events.kafka_consumer import ConsumedEvent, KafkaEventConsumer
from apps.api.telemetry.metrics import (
    CLICKHOUSE_EVENTS_NORMALIZED_TOTAL,
    CLICKHOUSE_EVENTS_REJECTED_TOTAL,
    CLICKHOUSE_RETRY_TOTAL,
)
from apps.api.telemetry.tracing import set_span_attributes

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

CLICKHOUSE_CONSUMER_GROUP = "voicemesh-clickhouse-analytics"
CLICKHOUSE_TOPICS = (
    "call-events",
    "pipeline-events",
    "provider-events",
    "usage-events",
    "billing-events",
    "tool-events",
    "webhook-events",
)


class ClickHouseAnalyticsConsumer:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client = ClickHouseCloudClient(settings)
        self._writer = ClickHouseBatchWriter(
            self._client,
            max_rows=settings.clickhouse_batch_max_rows,
            flush_seconds=settings.clickhouse_batch_flush_seconds,
        )
        self._consumer = KafkaEventConsumer(
            settings.kafka_bootstrap_servers,
            CLICKHOUSE_CONSUMER_GROUP,
            self.handle_batch,
            *CLICKHOUSE_TOPICS,
            batch_size=settings.clickhouse_batch_max_rows,
            batch_timeout_ms=int(settings.clickhouse_batch_flush_seconds * 1000),
        )

    async def run(self) -> None:
        if not self._settings.clickhouse_enabled:
            logger.info("ClickHouse analytics is disabled")
            return
        await self._client.ping()
        await self._consumer.run()

    async def handle_batch(self, batch: list[ConsumedEvent]) -> None:
        rows = []
        with tracer.start_as_current_span(
            "clickhouse.analytics.normalize_batch",
            kind=SpanKind.CONSUMER,
        ) as span:
            set_span_attributes(
                span,
                **{
                    "messaging.batch.message_count": len(batch),
                    "clickhouse.consumer_group": CLICKHOUSE_CONSUMER_GROUP,
                },
            )
            for item in batch:
                try:
                    row = normalize_consumed_event(item)
                except AnalyticsNormalizationError as exc:
                    CLICKHOUSE_EVENTS_REJECTED_TOTAL.labels(exc.reason_code).inc()
                    logger.warning(
                        "ClickHouse event rejected",
                        extra={
                            "reason_code": exc.reason_code,
                            "event_id": str(item.event.event_id),
                            "event_type": str(item.event.event_type),
                        },
                    )
                    continue
                rows.append(row)
                CLICKHOUSE_EVENTS_NORMALIZED_TOTAL.labels(row.event_type).inc()
        if not rows:
            return
        await self._insert_with_retry(rows)

    async def _insert_with_retry(self, rows: Iterable[object]) -> None:
        attempts = 0
        delay = 1.0
        while True:
            try:
                for row in rows:
                    await self._writer.add(row)  # type: ignore[arg-type]
                await self._writer.flush()
                return
            except Exception as exc:
                attempts += 1
                reason_code = _stable_retry_reason(exc)
                CLICKHOUSE_RETRY_TOTAL.labels(reason_code).inc()
                if delay > self._settings.clickhouse_max_retry_seconds:
                    raise
                sleep_for = min(delay, self._settings.clickhouse_max_retry_seconds)
                sleep_for += random.uniform(0, sleep_for * 0.2)
                logger.warning(
                    "Retrying ClickHouse insert",
                    extra={"attempts": attempts, "reason_code": reason_code},
                )
                await asyncio.sleep(sleep_for)
                delay *= 2

    async def close(self) -> None:
        await self._writer.close()
        await self._client.close()


def _stable_retry_reason(exc: Exception) -> str:
    name = exc.__class__.__name__.lower()
    if "timeout" in name:
        return "timeout"
    if "connect" in name or "network" in name:
        return "network_error"
    if "auth" in name or "permission" in name:
        return "auth_failed"
    return "insert_retry"
