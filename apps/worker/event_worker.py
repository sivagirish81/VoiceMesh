import asyncio
import logging

from apps.api.config import get_settings
from apps.api.db.outbox import OutboxPublisher
from apps.api.db.projector import EventProjector
from apps.api.db.repository import PostgresRepository
from apps.api.events.kafka_consumer import KafkaEventConsumer
from apps.api.events.kafka_producer import KafkaEventProducer
from apps.api.events.schemas import EventType, PipelineEvent
from apps.api.failure_injection.injector import FailureInjector
from apps.api.telemetry.tracing import configure_tracing

logger = logging.getLogger(__name__)


async def run_worker() -> None:
    settings = get_settings()
    configure_tracing("voicemesh-event-worker", settings.otel_exporter_otlp_endpoint)
    injector = FailureInjector(settings)
    repository = PostgresRepository(
        settings.database_url,
        injector,
        settings.database_pool_min_size,
        settings.database_pool_max_size,
        settings.database_command_timeout,
    )
    producer = KafkaEventProducer(settings.kafka_bootstrap_servers)
    await repository.connect()
    await producer.start()
    projector = EventProjector(
        repository,
        platform_rate_per_minute_usd=settings.billing_platform_rate_per_minute_usd,
        billing_pricing_version=settings.billing_pricing_version,
    )

    async def handle(event: PipelineEvent) -> None:
        inserted = await projector.project(event)
        if inserted is None:
            raise RuntimeError(f"Postgres projection failed for event {event.event_id}")
        if inserted is not False or event.event_type == EventType.DUPLICATE_EVENT_IGNORED:
            return
        duplicate = PipelineEvent.create(
            call_id=event.call_id,
            turn_id=event.turn_id,
            event_type=EventType.DUPLICATE_EVENT_IGNORED,
            stage="idempotency",
            sequence_number=event.sequence_number,
            idempotency_key=f"duplicate:{event.event_id}",
            payload={
                "duplicate_event_id": str(event.event_id),
                "duplicate_idempotency_key": event.idempotency_key,
            },
            trace_id=event.trace_id,
        )
        await producer.publish(duplicate)

    outbox = OutboxPublisher(repository, producer)
    outbox_task = asyncio.create_task(outbox.run())
    try:
        while True:
            consumer = KafkaEventConsumer(
                settings.kafka_bootstrap_servers,
                "voicemesh-postgres-projector-v1",
                handle,
                "call-events",
                "pipeline-events",
                "provider-events",
                "usage-events",
                "billing-events",
            )
            try:
                await consumer.run()
            except Exception:
                logger.exception("event projection consumer restarting")
                await asyncio.sleep(1)
    finally:
        outbox.stop()
        await outbox_task
        await producer.stop()
        await repository.close()


if __name__ == "__main__":
    asyncio.run(run_worker())
