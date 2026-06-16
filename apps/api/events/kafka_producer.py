import json
import logging
from typing import Any

from aiokafka import AIOKafkaProducer
from opentelemetry import trace
from opentelemetry.trace import SpanKind

from apps.api.events.schemas import PipelineEvent, topic_for_event
from apps.api.telemetry.tracing import (
    kafka_headers_from_current_context,
    set_span_attributes,
)

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class KafkaEventProducer:
    def __init__(self, bootstrap_servers: str) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            enable_idempotence=True,
            acks="all",
            value_serializer=lambda value: json.dumps(value, default=str).encode(),
        )
        self.started = False

    async def start(self) -> None:
        await self._producer.start()
        self.started = True

    async def stop(self) -> None:
        if self.started:
            await self._producer.stop()
            self.started = False

    async def publish(self, event: PipelineEvent, topic: str | None = None) -> None:
        destination = topic or topic_for_event(event.event_type)
        with tracer.start_as_current_span("kafka.publish", kind=SpanKind.PRODUCER) as span:
            set_span_attributes(
                span,
                **{
                    "messaging.system": "kafka",
                    "messaging.destination.name": destination,
                    "messaging.kafka.message_key": event.call_id,
                    "call_id": event.call_id,
                    "turn_id": event.turn_id,
                    "event_id": str(event.event_id),
                    "event_type": str(event.event_type),
                    "stage": event.stage,
                    "idempotency_key": event.idempotency_key,
                    "sequence_number": event.sequence_number,
                },
            )
            await self._producer.send_and_wait(
                destination,
                key=event.call_id.encode(),
                value=event.model_dump(mode="json"),
                headers=kafka_headers_from_current_context(),
            )

    async def publish_raw(self, topic: str, key: str, payload: dict[str, Any]) -> None:
        with tracer.start_as_current_span("kafka.publish.outbox", kind=SpanKind.PRODUCER) as span:
            event_type = payload.get("event_type")
            set_span_attributes(
                span,
                **{
                    "messaging.system": "kafka",
                    "messaging.destination.name": topic,
                    "messaging.kafka.message_key": key,
                    "call_id": payload.get("call_id", key),
                    "event_id": payload.get("event_id"),
                    "event_type": event_type,
                    "stage": payload.get("stage"),
                    "idempotency_key": payload.get("idempotency_key"),
                },
            )
            await self._producer.send_and_wait(
                topic,
                key=key.encode(),
                value=payload,
                headers=kafka_headers_from_current_context(),
            )


class NullEventProducer:
    """Used only by unit tests; application startup always creates real Kafka."""

    started = True

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def publish(self, event: PipelineEvent, topic: str | None = None) -> None:
        logger.debug("test event", extra={"event": event.model_dump(mode="json")})

    async def publish_raw(self, topic: str, key: str, payload: dict[str, Any]) -> None:
        logger.debug("test outbox event", extra={"topic": topic, "key": key})
