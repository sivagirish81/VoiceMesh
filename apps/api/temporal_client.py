import logging
from typing import Any

from temporalio.client import Client, WorkflowHandle
from temporalio.exceptions import WorkflowAlreadyStartedError

from apps.api.config import Settings

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
        await handle.signal(signal_name, event)

    async def health(self) -> bool:
        if not self.client:
            return False
        try:
            await self.client.service_client.check_health()
            return True
        except Exception:
            return False

