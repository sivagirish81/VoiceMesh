from typing import Any
from uuid import uuid4

import httpx

from apps.api.events.kafka_producer import KafkaEventProducer
from apps.api.events.schemas import EventType, PipelineEvent
from apps.api.temporal_client import TemporalLifecycleClient
from apps.api.tools.models import DurableToolConfig, ToolExecutionMode


class ToolExecutor:
    def __init__(
        self,
        *,
        producer: KafkaEventProducer,
        temporal: TemporalLifecycleClient,
    ) -> None:
        self._producer = producer
        self._temporal = temporal

    async def execute(
        self,
        *,
        tenant_id: str,
        assistant_id: str,
        call_id: str,
        turn_id: str,
        tool_name: str,
        mode: ToolExecutionMode,
        arguments: dict[str, Any],
        config: dict[str, Any],
        tool_invocation_id: str | None = None,
    ) -> dict[str, Any]:
        invocation_id = tool_invocation_id or f"tool_{uuid4()}"
        if mode == ToolExecutionMode.SYNC_DIRECT:
            return await self._execute_sync_direct(
                tenant_id,
                assistant_id,
                call_id,
                turn_id,
                tool_name,
                invocation_id,
                arguments,
                config,
            )
        if mode == ToolExecutionMode.ASYNC_JOB:
            await self._emit_tool_event(
                EventType.TOOL_ACTION_ACCEPTED,
                tenant_id,
                assistant_id,
                call_id,
                turn_id,
                tool_name,
                invocation_id,
                {"mode": str(mode), "accepted": True},
            )
            return {
                "tool_invocation_id": invocation_id,
                "status": "ACCEPTED",
                "message": "Accepted for asynchronous processing.",
            }
        durable_config = DurableToolConfig.model_validate(config)
        workflow_id = await self._temporal.start_durable_action(
            {
                "tenant_id": tenant_id,
                "assistant_id": assistant_id,
                "call_id": call_id,
                "turn_id": turn_id,
                "tool_invocation_id": invocation_id,
                "tool_name": tool_name,
                "tool_arguments": arguments,
                "tool_config": durable_config.model_dump(mode="json"),
            }
        )
        return {
            "tool_invocation_id": invocation_id,
            "workflow_id": workflow_id,
            "status": "ACCEPTED",
            "message": "I've started that request. It is being processed now.",
        }

    async def _execute_sync_direct(
        self,
        tenant_id: str,
        assistant_id: str,
        call_id: str,
        turn_id: str,
        tool_name: str,
        invocation_id: str,
        arguments: dict[str, Any],
        config: dict[str, Any],
    ) -> dict[str, Any]:
        url = str(config["url"])
        method = str(config.get("method", "POST"))
        try:
            async with httpx.AsyncClient(timeout=float(config.get("timeout_seconds", 2))) as client:
                response = await client.request(method, url, json=arguments)
                response.raise_for_status()
                payload = response.json() if response.content else {}
            await self._emit_tool_event(
                EventType.TOOL_ACTION_COMPLETED,
                tenant_id,
                assistant_id,
                call_id,
                turn_id,
                tool_name,
                invocation_id,
                {"mode": "SYNC_DIRECT", "result": payload},
            )
            return {"tool_invocation_id": invocation_id, "status": "COMPLETED", "result": payload}
        except Exception as exc:
            await self._emit_tool_event(
                EventType.TOOL_ACTION_FAILED,
                tenant_id,
                assistant_id,
                call_id,
                turn_id,
                tool_name,
                invocation_id,
                {"mode": "SYNC_DIRECT", "error": str(exc)},
            )
            return {"tool_invocation_id": invocation_id, "status": "FAILED", "error": str(exc)}

    async def _emit_tool_event(
        self,
        event_type: EventType,
        tenant_id: str,
        assistant_id: str,
        call_id: str,
        turn_id: str,
        tool_name: str,
        invocation_id: str,
        payload: dict[str, Any],
    ) -> None:
        await self._producer.publish(
            PipelineEvent.create(
                call_id=call_id,
                turn_id=turn_id,
                event_type=event_type,
                stage="tool",
                sequence_number=1,
                idempotency_key=f"{invocation_id}:{event_type}",
                payload={
                    "event_version": 1,
                    "tenant_id": tenant_id,
                    "assistant_id": assistant_id,
                    "tool_invocation_id": invocation_id,
                    "tool_name": tool_name,
                    **payload,
                },
            )
        )
