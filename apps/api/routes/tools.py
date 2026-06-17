from typing import Any, cast

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from apps.api.events.kafka_producer import KafkaEventProducer
from apps.api.temporal_client import TemporalLifecycleClient
from apps.api.tools.executor import ToolExecutor
from apps.api.tools.models import ToolExecutionMode

router = APIRouter(prefix="/tools", tags=["tools"])


class ToolExecutionRequest(BaseModel):
    tenant_id: str = "local-demo-tenant"
    assistant_id: str = "local-demo-assistant"
    call_id: str
    turn_id: str = "tool-demo-turn"
    tool_invocation_id: str | None = None
    tool_name: str
    mode: ToolExecutionMode
    arguments: dict[str, Any] = Field(default_factory=dict)
    config: dict[str, Any] = Field(default_factory=dict)


@router.post("/execute")
async def execute_tool(body: ToolExecutionRequest, request: Request) -> dict[str, Any]:
    executor = ToolExecutor(
        producer=cast(KafkaEventProducer, request.app.state.producer),
        temporal=cast(TemporalLifecycleClient, request.app.state.temporal),
    )
    return await executor.execute(
        tenant_id=body.tenant_id,
        assistant_id=body.assistant_id,
        call_id=body.call_id,
        turn_id=body.turn_id,
        tool_name=body.tool_name,
        mode=body.mode,
        arguments=body.arguments,
        config=body.config,
        tool_invocation_id=body.tool_invocation_id,
    )


@router.post("/durable-actions/{tool_invocation_id}/cancel")
async def cancel_durable_action(
    tool_invocation_id: str,
    request: Request,
    reason: str = "requested_by_user",
) -> dict[str, Any]:
    temporal = cast(TemporalLifecycleClient, request.app.state.temporal)
    await temporal.signal_durable_action(
        tool_invocation_id,
        "CancelRequested",
        {"tool_invocation_id": tool_invocation_id, "reason": reason},
    )
    return {"tool_invocation_id": tool_invocation_id, "cancel_requested": True}
