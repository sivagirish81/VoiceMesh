import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from apps.api.config import get_settings
from apps.api.telemetry.tracing import configure_tracing
from apps.worker.activities import (
    emit_recovery_event,
    mark_call_completed,
    mark_call_failed,
    persist_call_state,
    select_fallback_provider,
    summarize_call,
)
from apps.worker.workflows import CallWorkflow


async def run_worker() -> None:
    settings = get_settings()
    configure_tracing("voicemesh-temporal-worker", settings.otel_exporter_otlp_endpoint)
    client = await Client.connect(
        settings.temporal_address, namespace=settings.temporal_namespace
    )
    worker = Worker(
        client,
        task_queue=settings.temporal_task_queue,
        workflows=[CallWorkflow],
        activities=[
            persist_call_state,
            select_fallback_provider,
            emit_recovery_event,
            summarize_call,
            mark_call_completed,
            mark_call_failed,
        ],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(run_worker())

