from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class EventType(StrEnum):
    CALL_STARTED = "call.started"
    CALL_ENDED = "call.ended"
    CALL_FAILED = "call.failed"
    VAD_SPEECH_STARTED = "vad.speech_started"
    VAD_SPEECH_ENDED = "vad.speech_ended"
    STT_STARTED = "stt.started"
    STT_FINAL_TRANSCRIPT = "stt.final_transcript"
    LLM_STARTED = "llm.started"
    LLM_FIRST_TOKEN = "llm.first_token"
    LLM_TOKEN = "llm.token"
    LLM_FINAL_RESPONSE = "llm.final_response"
    TTS_STARTED = "tts.started"
    TTS_FIRST_AUDIO = "tts.first_audio"
    TTS_AUDIO_CHUNK = "tts.audio_chunk"
    TTS_COMPLETED = "tts.completed"
    TRANSPORT_AUDIO_SENT = "transport.audio_sent"
    PIPELINE_CORKED = "pipeline.corked"
    PIPELINE_UNCORKED = "pipeline.uncorked"
    PIPELINE_STAGE_TIMEOUT = "pipeline.stage_timeout"
    PROVIDER_FAILED = "provider.failed"
    PROVIDER_FALLBACK_SELECTED = "provider.fallback_selected"
    DUPLICATE_EVENT_IGNORED = "duplicate_event.ignored"
    POSTGRES_WRITE_FAILED = "postgres.write_failed"
    POSTGRES_RECOVERED = "postgres.recovered"
    WORKFLOW_STATE_CHANGED = "workflow.state_changed"
    USAGE_STT_RECORDED = "usage.stt"
    USAGE_LLM_RECORDED = "usage.llm"
    USAGE_TTS_RECORDED = "usage.tts"
    BILLING_USAGE_RECORDED = "billing.usage_recorded"
    BILLING_FINALIZED = "billing.finalized"
    TOOL_ACTION_REQUESTED = "tool.action.requested"
    TOOL_ACTION_CREATE_STARTED = "tool.action.create_started"
    TOOL_ACTION_ACCEPTED = "tool.action.accepted"
    TOOL_ACTION_CANCEL_REQUESTED = "tool.action.cancel_requested"
    TOOL_ACTION_CANCELLED = "tool.action.cancelled"
    TOOL_ACTION_CANNOT_CANCEL = "tool.action.cannot_cancel"
    TOOL_ACTION_COMPLETED = "tool.action.completed"
    TOOL_ACTION_FAILED = "tool.action.failed"
    TOOL_ACTION_TIMED_OUT = "tool.action.timed_out"
    WEBHOOK_DELIVERY_REQUESTED = "webhook.delivery_requested"
    WEBHOOK_DELIVERED = "webhook.delivered"
    WEBHOOK_FAILED = "webhook.failed"
    WORKFLOW_DONE = "workflow.done"


class PipelineEvent(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    call_id: str = Field(min_length=1)
    turn_id: str = Field(min_length=1)
    event_type: EventType
    stage: str = Field(min_length=1)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    sequence_number: int = Field(ge=1)
    idempotency_key: str = Field(min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    trace_id: str | None = None

    @classmethod
    def create(
        cls,
        *,
        call_id: str,
        turn_id: str,
        event_type: EventType,
        stage: str,
        sequence_number: int,
        payload: dict[str, Any] | None = None,
        trace_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> "PipelineEvent":
        return cls(
            call_id=call_id,
            turn_id=turn_id,
            event_type=event_type,
            stage=stage,
            sequence_number=sequence_number,
            idempotency_key=idempotency_key
            or f"{call_id}:{turn_id}:{event_type}:{sequence_number}",
            payload=payload or {},
            trace_id=trace_id,
        )


TOPIC_BY_EVENT_PREFIX = {
    "call.": "call-events",
    "vad.": "pipeline-events",
    "stt.": "pipeline-events",
    "llm.": "pipeline-events",
    "tts.": "pipeline-events",
    "transport.": "pipeline-events",
    "pipeline.": "pipeline-events",
    "provider.": "provider-events",
    "duplicate_event.": "pipeline-events",
    "postgres.": "pipeline-events",
    "workflow.": "call-events",
    "usage.": "usage-events",
    "billing.": "billing-events",
    "tool.": "tool-events",
    "webhook.": "webhook-events",
}


def topic_for_event(event_type: EventType) -> str:
    value = str(event_type)
    for prefix, topic in TOPIC_BY_EVENT_PREFIX.items():
        if value.startswith(prefix):
            return topic
    return "dead-letter-events"
