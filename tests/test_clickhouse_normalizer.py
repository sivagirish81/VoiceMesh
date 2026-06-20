from apps.api.analytics.clickhouse.normalizer import normalize_pipeline_event
from apps.api.events.schemas import EventType, PipelineEvent


def test_normalizer_maps_latency_and_provider_fields() -> None:
    event = PipelineEvent.create(
        call_id="call-1",
        turn_id="turn-1",
        event_type=EventType.LLM_FIRST_TOKEN,
        stage="llm",
        sequence_number=1,
        payload={
            "tenant_id": "tenant-a",
            "assistant_id": "assistant-a",
            "provider": "openai",
            "model": "gpt-4.1-mini",
            "latency_ms": 312,
        },
    )

    row = normalize_pipeline_event(event)

    assert row.tenant_id == "tenant-a"
    assert row.assistant_id == "assistant-a"
    assert row.stage == "llm"
    assert row.provider == "openai"
    assert row.model == "gpt-4.1-mini"
    assert row.latency_ms == 312


def test_normalizer_redacts_transcript_payload_text() -> None:
    event = PipelineEvent.create(
        call_id="call-1",
        turn_id="turn-1",
        event_type=EventType.STT_FINAL_TRANSCRIPT,
        stage="stt",
        sequence_number=1,
        payload={"transcript": "my account number is 123", "latency_ms": 500},
    )

    row = normalize_pipeline_event(event)

    assert "my account number" not in row.payload_json
    assert "[redacted]" in row.payload_json


def test_provider_error_reason_code_is_stable() -> None:
    event = PipelineEvent.create(
        call_id="call-1",
        turn_id="turn-1",
        event_type=EventType.PROVIDER_FAILED,
        stage="tts",
        sequence_number=1,
        payload={"error": "Timeout calling https://provider.example.com/request/abc123"},
    )

    row = normalize_pipeline_event(event)

    assert row.reason_code == "timeout"
    assert "provider.example.com" not in row.reason_code
