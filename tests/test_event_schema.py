import pytest
from pydantic import ValidationError

from apps.api.events.schemas import EventType, PipelineEvent, topic_for_event


def test_event_schema_contains_required_fields() -> None:
    event = PipelineEvent.create(
        call_id="call-1",
        turn_id="turn-1",
        event_type=EventType.STT_FINAL_TRANSCRIPT,
        stage="stt",
        sequence_number=1,
        payload={"transcript": "hello"},
    )
    data = event.model_dump(mode="json")
    assert set(data) == {
        "event_id",
        "call_id",
        "turn_id",
        "event_type",
        "stage",
        "timestamp",
        "sequence_number",
        "idempotency_key",
        "payload",
        "trace_id",
    }
    assert topic_for_event(event.event_type) == "pipeline-events"


def test_sequence_number_must_be_positive() -> None:
    with pytest.raises(ValidationError):
        PipelineEvent.create(
            call_id="call-1",
            turn_id="turn-1",
            event_type=EventType.CALL_STARTED,
            stage="transport",
            sequence_number=0,
        )


def test_usage_and_billing_events_have_dedicated_topics() -> None:
    assert topic_for_event(EventType.USAGE_STT_RECORDED) == "usage-events"
    assert topic_for_event(EventType.USAGE_LLM_RECORDED) == "usage-events"
    assert topic_for_event(EventType.USAGE_TTS_RECORDED) == "usage-events"
    assert topic_for_event(EventType.BILLING_USAGE_RECORDED) == "billing-events"
    assert topic_for_event(EventType.BILLING_FINALIZED) == "billing-events"
    assert topic_for_event(EventType.TOOL_ACTION_REQUESTED) == "tool-events"
    assert topic_for_event(EventType.WEBHOOK_DELIVERED) == "webhook-events"
