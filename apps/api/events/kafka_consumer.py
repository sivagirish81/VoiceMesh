import json
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import TopicPartition
from opentelemetry import trace
from opentelemetry.trace import SpanKind

from apps.api.events.schemas import EventType, PipelineEvent
from apps.api.telemetry.metrics import (
    KAFKA_BATCH_DURATION,
    KAFKA_BATCH_SIZE,
    KAFKA_CONSUMER_LAG,
    KAFKA_EVENTS_CONSUMED_TOTAL,
)
from apps.api.telemetry.tracing import context_from_kafka_headers, set_span_attributes

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


@dataclass(frozen=True)
class ConsumedEvent:
    event: PipelineEvent
    topic: str
    partition: int
    offset: int
    headers: list[tuple[str, bytes]]


class KafkaEventConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        handler: Callable[[list[ConsumedEvent]], Awaitable[None]],
        *topics: str,
        batch_size: int = 100,
        batch_timeout_ms: int = 500,
    ) -> None:
        self._handler = handler
        self._group_id = group_id
        self._batch_size = batch_size
        self._batch_timeout_ms = batch_timeout_ms
        self._consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )

    async def run(self) -> None:
        await self._consumer.start()
        try:
            buffered: list[ConsumedEvent] = []
            batch_deadline = time.monotonic() + self._batch_timeout_ms / 1000
            while True:
                remaining_ms = max(1, int((batch_deadline - time.monotonic()) * 1000))
                records = await self._consumer.getmany(
                    timeout_ms=remaining_ms,
                    max_records=max(1, self._batch_size - len(buffered)),
                )
                self._observe_lag(records)
                decoded = self._decode_records(records)
                buffered.extend(decoded)
                barrier_seen = any(
                    item.event.event_type == EventType.USAGE_FINALIZATION_BARRIER
                    for item in decoded
                )
                timed_out = time.monotonic() >= batch_deadline
                batch_full = len(buffered) >= self._batch_size
                if not buffered:
                    if timed_out:
                        batch_deadline = time.monotonic() + self._batch_timeout_ms / 1000
                    continue
                if not (barrier_seen or timed_out or batch_full):
                    continue
                batch = buffered
                buffered = []
                batch_deadline = time.monotonic() + self._batch_timeout_ms / 1000
                with tracer.start_as_current_span(
                    "kafka.consume.batch",
                    kind=SpanKind.CONSUMER,
                ) as span:
                    try:
                        started = time.perf_counter()
                        KAFKA_BATCH_SIZE.labels(self._group_id).observe(len(batch))
                        set_span_attributes(
                            span,
                            **{
                                "messaging.system": "kafka",
                                "messaging.kafka.consumer_group": self._group_id,
                                "messaging.batch.message_count": len(batch),
                                "messaging.batch.contains_barrier": any(
                                    item.event.event_type == EventType.USAGE_FINALIZATION_BARRIER
                                    for item in batch
                                ),
                            },
                        )
                        await self._handler(batch)
                        await self._commit_batch(batch)
                        KAFKA_BATCH_DURATION.labels(self._group_id).observe(
                            time.perf_counter() - started
                        )
                    except Exception:
                        logger.exception("Kafka event batch handling failed")
                        raise
        finally:
            await self._consumer.stop()

    def _decode_records(self, records: dict[TopicPartition, list[Any]]) -> list[ConsumedEvent]:
        batch: list[ConsumedEvent] = []
        for _topic_partition, messages in records.items():
            for message in messages:
                parent_context = context_from_kafka_headers(message.headers)
                with tracer.start_as_current_span(
                    "kafka.consume.decode",
                    context=parent_context,
                    kind=SpanKind.CONSUMER,
                ) as span:
                    event = PipelineEvent.model_validate(json.loads(message.value))
                    KAFKA_EVENTS_CONSUMED_TOTAL.labels(
                        message.topic,
                        str(event.event_type),
                        self._group_id,
                    ).inc()
                    set_span_attributes(
                        span,
                        **{
                            "messaging.system": "kafka",
                            "messaging.destination.name": message.topic,
                            "messaging.kafka.partition": message.partition,
                            "messaging.kafka.offset": message.offset,
                            "messaging.kafka.consumer_group": self._group_id,
                            "call_id": event.call_id,
                            "turn_id": event.turn_id,
                            "event_id": str(event.event_id),
                            "event_type": str(event.event_type),
                            "stage": event.stage,
                            "idempotency_key": event.idempotency_key,
                            "sequence_number": event.sequence_number,
                        },
                    )
                batch.append(
                    ConsumedEvent(
                        event=event,
                        topic=message.topic,
                        partition=message.partition,
                        offset=message.offset,
                        headers=list(message.headers or []),
                    )
                )
        return batch

    def _observe_lag(self, records: dict[TopicPartition, list[Any]]) -> None:
        for topic_partition, messages in records.items():
            if not messages:
                continue
            highwater = self._consumer.highwater(topic_partition)
            if highwater is None:
                continue
            last_offset = max(message.offset for message in messages)
            lag = max(0, int(highwater) - int(last_offset) - 1)
            KAFKA_CONSUMER_LAG.labels(
                topic_partition.topic,
                str(topic_partition.partition),
                self._group_id,
            ).set(lag)

    async def _commit_batch(self, batch: list[ConsumedEvent]) -> None:
        offsets: dict[TopicPartition, int] = {}
        for item in batch:
            topic_partition = TopicPartition(item.topic, item.partition)
            offsets[topic_partition] = max(offsets.get(topic_partition, 0), item.offset + 1)
        await self._consumer.commit(offsets)
