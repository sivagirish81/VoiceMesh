import asyncio
import logging

from apps.api.config import get_settings
from apps.api.db.outbox import OutboxPublisher
from apps.api.db.projector import EventProjector
from apps.api.db.repository import PostgresRepository
from apps.api.events.kafka_consumer import ConsumedEvent, KafkaEventConsumer
from apps.api.events.kafka_producer import KafkaEventProducer
from apps.api.events.schemas import EventType, PipelineEvent
from apps.api.failure_injection.injector import FailureInjector
from apps.api.telemetry.metrics import start_metrics_http_server
from apps.api.telemetry.tracing import configure_tracing
from apps.api.temporal_client import TemporalLifecycleClient

logger = logging.getLogger(__name__)
CONSUMER_GROUP = "voicemesh-postgres-projector-v1"


async def run_worker() -> None:
    settings = get_settings()
    configure_tracing("voicemesh-event-worker", settings.otel_exporter_otlp_endpoint)
    start_metrics_http_server(settings.event_worker_metrics_port)
    injector = FailureInjector(settings)
    repository = PostgresRepository(
        settings.database_url,
        injector,
        settings.database_pool_min_size,
        settings.database_pool_max_size,
        settings.database_command_timeout,
    )
    producer = KafkaEventProducer(settings.kafka_bootstrap_servers)
    temporal = TemporalLifecycleClient(settings)
    await repository.connect()
    await producer.start()
    await temporal.connect()
    projector = EventProjector(
        repository,
        platform_rate_per_minute_usd=settings.billing_platform_rate_per_minute_usd,
        billing_pricing_version=settings.billing_pricing_version,
    )

    def required_usage_types() -> list[str]:
        return [
            item.strip()
            for item in settings.billing_required_usage_types.split(",")
            if item.strip()
        ]

    async def signal_usage_projection(call_id: str, trace_id: str | None) -> None:
        try:
            await temporal.signal_billing(
                call_id,
                "UsageProjectionUpdated",
                {"call_id": call_id, "trace_id": trace_id},
            )
        except Exception:
            logger.info(
                "billing workflow not ready for usage projection signal",
                extra={"call_id": call_id},
            )

    async def handle_batch(consumed_events: list[ConsumedEvent]) -> None:
        result = await projector.project_batch(consumed_events, consumer_group=CONSUMER_GROUP)
        if result is None:
            raise RuntimeError("Postgres batch projection failed")

        trace_by_call = {
            event.call_id: event.trace_id for event in result.inserted_events if event.trace_id
        }
        for event in result.call_ended_events:
            await temporal.start_billing_finalization(
                event.call_id,
                {
                    "tenant_id": event.payload.get("tenant_id", "local-demo-tenant"),
                    "assistant_id": event.payload.get("assistant_id", "local-demo-assistant"),
                    "call_id": event.call_id,
                    "required_usage_types": required_usage_types(),
                    "wait_timeout_seconds": settings.billing_usage_wait_seconds,
                    "settle_seconds": settings.billing_usage_settle_seconds,
                    "missing_usage_policy": settings.billing_missing_usage_policy,
                    "pricing_version": settings.billing_pricing_version,
                    "call_ended": True,
                    "consumer_group": CONSUMER_GROUP,
                    "trace_id": event.trace_id,
                },
            )
            await temporal.signal_billing(
                event.call_id,
                "CallEnded",
                {"call_id": event.call_id, "trace_id": event.trace_id},
            )

        for call_id in result.usage_projection_call_ids:
            await signal_usage_projection(call_id, trace_by_call.get(call_id))

        for event in result.late_usage_events:
            await temporal.start_billing_adjustment(
                {
                    "tenant_id": event.payload.get("tenant_id", "local-demo-tenant"),
                    "assistant_id": event.payload.get("assistant_id", "local-demo-assistant"),
                    "call_id": event.call_id,
                    "source_event_id": str(event.event_id),
                    "reason": "late_usage_after_finalization",
                    "trace_id": event.trace_id,
                }
            )

        for event in result.duplicate_events:
            if event.event_type == EventType.DUPLICATE_EVENT_IGNORED:
                continue
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
                CONSUMER_GROUP,
                handle_batch,
                "call-events",
                "pipeline-events",
                "provider-events",
                "usage-events",
                "billing-events",
                batch_size=settings.kafka_consumer_batch_size,
                batch_timeout_ms=settings.kafka_consumer_batch_timeout_ms,
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
