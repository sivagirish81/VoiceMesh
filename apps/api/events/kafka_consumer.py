import json
import logging
from collections.abc import Awaitable, Callable

from aiokafka import AIOKafkaConsumer
from opentelemetry import trace
from opentelemetry.trace import SpanKind

from apps.api.events.schemas import PipelineEvent
from apps.api.telemetry.tracing import context_from_kafka_headers, set_span_attributes

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class KafkaEventConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        handler: Callable[[PipelineEvent], Awaitable[None]],
        *topics: str,
    ) -> None:
        self._handler = handler
        self._group_id = group_id
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
            async for message in self._consumer:
                parent_context = context_from_kafka_headers(message.headers)
                topic = message.topic
                with tracer.start_as_current_span(
                    "kafka.consume",
                    context=parent_context,
                    kind=SpanKind.CONSUMER,
                ) as span:
                    try:
                        event = PipelineEvent.model_validate(json.loads(message.value))
                        set_span_attributes(
                            span,
                            **{
                                "messaging.system": "kafka",
                                "messaging.destination.name": topic,
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
                        await self._handler(event)
                        await self._consumer.commit()
                    except Exception:
                        logger.exception("Kafka event handling failed")
                        raise
        finally:
            await self._consumer.stop()
