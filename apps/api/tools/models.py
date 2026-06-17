from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class ToolExecutionMode(StrEnum):
    SYNC_DIRECT = "SYNC_DIRECT"
    ASYNC_JOB = "ASYNC_JOB"
    DURABLE_ACTION = "DURABLE_ACTION"


class DurableActionState(StrEnum):
    REQUESTED = "REQUESTED"
    CREATE_IN_FLIGHT = "CREATE_IN_FLIGHT"
    ACTIVE = "ACTIVE"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    CANCEL_IN_FLIGHT = "CANCEL_IN_FLIGHT"
    CANCELLED = "CANCELLED"
    COMPLETED = "COMPLETED"
    CANNOT_CANCEL = "CANNOT_CANCEL"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"


class BillingWorkflowState(StrEnum):
    WAITING_FOR_CALL_END = "WAITING_FOR_CALL_END"
    WAITING_FOR_MANIFEST = "WAITING_FOR_MANIFEST"
    WAITING_FOR_PROJECTION = "WAITING_FOR_PROJECTION"
    WAITING_FOR_USAGE = "WAITING_FOR_USAGE"
    FINALIZING = "FINALIZING"
    FINALIZED = "FINALIZED"
    FINALIZED_WITH_WARNINGS = "FINALIZED_WITH_WARNINGS"
    FAILED = "FAILED"
    NEEDS_REVIEW = "NEEDS_REVIEW"


class WebhookDeliveryState(StrEnum):
    REQUESTED = "REQUESTED"
    DELIVERING = "DELIVERING"
    DELIVERED = "DELIVERED"
    FAILED = "FAILED"


class ExternalHttpConfig(BaseModel):
    method: str = "POST"
    url: str | None = None
    url_template: str | None = None
    idempotency_key_template: str | None = None
    timeout_seconds: float = Field(default=10, gt=0)


class ResponseMapping(BaseModel):
    external_request_id: str = "$.id"
    status: str = "$.status"
    message: str = "$.message"


class DurableToolConfig(BaseModel):
    tool_name: str
    mode: ToolExecutionMode = ToolExecutionMode.DURABLE_ACTION
    create: ExternalHttpConfig
    cancel: ExternalHttpConfig | None = None
    status: ExternalHttpConfig | None = None
    response_mapping: ResponseMapping = Field(default_factory=ResponseMapping)
    terminal_states: list[str] = Field(
        default_factory=lambda: ["completed", "cancelled", "cannot_cancel", "failed"]
    )
    poll_interval_seconds: int = Field(default=10, ge=1)
    max_runtime_seconds: int = Field(default=3600, ge=1)


class DurableActionInput(BaseModel):
    tenant_id: str = "local-demo-tenant"
    assistant_id: str = "local-demo-assistant"
    call_id: str
    turn_id: str
    tool_invocation_id: str
    tool_name: str
    tool_arguments: dict[str, Any] = Field(default_factory=dict)
    tool_config: DurableToolConfig
    trace_id: str | None = None


class DurableActionStatus(BaseModel):
    state: DurableActionState
    external_request_id: str | None = None
    message: str | None = None
    cancel_requested: bool = False
    last_error: str | None = None


class BillingFinalizationInput(BaseModel):
    tenant_id: str = "local-demo-tenant"
    assistant_id: str = "local-demo-assistant"
    call_id: str
    required_usage_types: list[str]
    wait_timeout_seconds: int = 20
    settle_seconds: int = 3
    missing_usage_policy: BillingWorkflowState = BillingWorkflowState.FINALIZED_WITH_WARNINGS
    pricing_version: str = "local-demo-v1"
    consumer_group: str = "voicemesh-postgres-projector-v1"
    trace_id: str | None = None


class BillingStatus(BaseModel):
    state: BillingWorkflowState
    present_usage_types: list[str] = Field(default_factory=list)
    missing_usage_types: list[str] = Field(default_factory=list)
    manifest_present: bool = False
    projection_caught_up: bool = False
    missing_expectations: list[str] = Field(default_factory=list)
    total_cost_cents: int | None = None
    warnings: list[str] = Field(default_factory=list)


class WebhookDeliveryInput(BaseModel):
    tenant_id: str = "local-demo-tenant"
    assistant_id: str = "local-demo-assistant"
    call_id: str
    webhook_delivery_id: str
    target_url: str
    event_type: str = "end_of_call_report"
    payload: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str
    max_attempts: int = Field(default=5, ge=1)
    backoff_seconds: int = Field(default=2, ge=0)
    trace_id: str | None = None
