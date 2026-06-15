import json
import logging
from collections.abc import Awaitable, Callable

from aiokafka import AIOKafkaConsumer
from opentelemetry import trace

from apps.api.events.schemas import PipelineEvent

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
                with tracer.start_as_current_span("kafka.consume"):
                    try:
                        event = PipelineEvent.model_validate(json.loads(message.value))
                        await self._handler(event)
                        await self._consumer.commit()
                    except Exception:
                        logger.exception("Kafka event handling failed")
                        raise
        finally:
            await self._consumer.stop()
