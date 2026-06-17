import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from apps.api.config import get_settings
from apps.api.telemetry.tracing import configure_tracing
from apps.worker.activities import (
    cancel_external_action,
    create_external_action,
    deliver_webhook,
    emit_recovery_event,
    emit_tool_event,
    emit_workflow_event,
    finalize_call_billing,
    get_external_action_status,
    load_billing_readiness,
    mark_call_completed,
    mark_call_failed,
    persist_call_state,
    persist_tool_state,
    persist_webhook_delivery,
    select_fallback_provider,
    summarize_call,
)
from apps.worker.durable_workflows import (
    BillingFinalizationWorkflow,
    CallCompletionWorkflow,
    DurableActionWorkflow,
    WebhookDeliveryWorkflow,
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
        workflows=[
            CallWorkflow,
            DurableActionWorkflow,
            BillingFinalizationWorkflow,
            WebhookDeliveryWorkflow,
            CallCompletionWorkflow,
        ],
        activities=[
            persist_call_state,
            select_fallback_provider,
            emit_recovery_event,
            summarize_call,
            mark_call_completed,
            mark_call_failed,
            persist_tool_state,
            emit_tool_event,
            create_external_action,
            cancel_external_action,
            get_external_action_status,
            load_billing_readiness,
            finalize_call_billing,
            emit_workflow_event,
            persist_webhook_delivery,
            deliver_webhook,
        ],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(run_worker())
