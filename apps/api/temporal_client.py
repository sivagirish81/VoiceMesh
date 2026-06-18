import logging
from typing import Any

from temporalio.client import Client, WorkflowHandle
from temporalio.exceptions import WorkflowAlreadyStartedError

from apps.api.config import Settings
from apps.api.telemetry.metrics import TEMPORAL_WORKFLOWS_TOTAL
from apps.api.telemetry.tracing import inject_trace_context

logger = logging.getLogger(__name__)


class TemporalLifecycleClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self.client: Client | None = None
        self._handles: dict[str, WorkflowHandle[Any, Any]] = {}

    async def connect(self) -> None:
        self.client = await Client.connect(
            self._settings.temporal_address,
            namespace=self._settings.temporal_namespace,
        )

    async def start_call(self, call_id: str) -> None:
        if not self.client:
            raise RuntimeError("Temporal client not connected")
        try:
            handle = await self.client.start_workflow(
                "CallWorkflow",
                {"call_id": call_id, "timeout_seconds": 3600},
                id=f"call-{call_id}",
                task_queue=self._settings.temporal_task_queue,
            )
            TEMPORAL_WORKFLOWS_TOTAL.labels("CallWorkflow", "started").inc()
            self._handles[call_id] = handle
        except WorkflowAlreadyStartedError:
            self._handles[call_id] = self.client.get_workflow_handle(f"call-{call_id}")
        await self.signal(call_id, "call_started", {"call_id": call_id})

    async def signal(self, call_id: str, signal_name: str, event: dict[str, Any]) -> None:
        if not self.client:
            logger.warning("Temporal unavailable; lifecycle signal skipped")
            return
        handle = self._handles.get(call_id) or self.client.get_workflow_handle(f"call-{call_id}")
        self._handles[call_id] = handle
        await handle.signal(signal_name, inject_trace_context(event))

    async def start_durable_action(self, input_data: dict[str, Any]) -> str:
        if not self.client:
            raise RuntimeError("Temporal client not connected")
        workflow_id = f"tool-{input_data['tool_invocation_id']}"
        try:
            await self.client.start_workflow(
                "DurableActionWorkflow",
                inject_trace_context(input_data),
                id=workflow_id,
                task_queue=self._settings.temporal_task_queue,
            )
            TEMPORAL_WORKFLOWS_TOTAL.labels("DurableActionWorkflow", "started").inc()
        except WorkflowAlreadyStartedError:
            pass
        return workflow_id

    async def signal_durable_action(
        self,
        tool_invocation_id: str,
        signal_name: str,
        event: dict[str, Any],
    ) -> None:
        if not self.client:
            logger.warning("Temporal unavailable; durable action signal skipped")
            return
        handle = self.client.get_workflow_handle(f"tool-{tool_invocation_id}")
        await handle.signal(signal_name, inject_trace_context(event))

    async def start_billing_finalization(
        self,
        call_id: str,
        input_data: dict[str, Any],
    ) -> str:
        if not self.client:
            raise RuntimeError("Temporal client not connected")
        workflow_id = f"billing-{call_id}"
        try:
            await self.client.start_workflow(
                "BillingFinalizationWorkflow",
                inject_trace_context(input_data),
                id=workflow_id,
                task_queue=self._settings.temporal_task_queue,
            )
            TEMPORAL_WORKFLOWS_TOTAL.labels("BillingFinalizationWorkflow", "started").inc()
        except WorkflowAlreadyStartedError:
            pass
        return workflow_id

    async def signal_billing(
        self,
        call_id: str,
        signal_name: str,
        event: dict[str, Any],
    ) -> None:
        if not self.client:
            logger.warning("Temporal unavailable; billing signal skipped")
            return
        handle = self.client.get_workflow_handle(f"billing-{call_id}")
        await handle.signal(signal_name, inject_trace_context(event))

    async def start_billing_adjustment(self, input_data: dict[str, Any]) -> str:
        if not self.client:
            raise RuntimeError("Temporal client not connected")
        workflow_id = (
            f"billing-adjustment-{input_data['call_id']}-{input_data['source_event_id']}"
        )
        try:
            await self.client.start_workflow(
                "BillingAdjustmentWorkflow",
                inject_trace_context(input_data),
                id=workflow_id,
                task_queue=self._settings.temporal_task_queue,
            )
            TEMPORAL_WORKFLOWS_TOTAL.labels("BillingAdjustmentWorkflow", "started").inc()
        except WorkflowAlreadyStartedError:
            pass
        return workflow_id

    async def start_webhook_delivery(self, input_data: dict[str, Any]) -> str:
        if not self.client:
            raise RuntimeError("Temporal client not connected")
        workflow_id = f"webhook-{input_data['webhook_delivery_id']}"
        try:
            await self.client.start_workflow(
                "WebhookDeliveryWorkflow",
                inject_trace_context(input_data),
                id=workflow_id,
                task_queue=self._settings.temporal_task_queue,
            )
            TEMPORAL_WORKFLOWS_TOTAL.labels("WebhookDeliveryWorkflow", "started").inc()
        except WorkflowAlreadyStartedError:
            pass
        return workflow_id

    async def health(self) -> bool:
        if not self.client:
            return False
        try:
            await self.client.service_client.check_health()
            return True
        except Exception:
            return False
