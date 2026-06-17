import pytest

from apps.api.tools.http_config import cents
from apps.api.tools.models import BillingWorkflowState
from apps.worker.activities import _billing_component_costs, load_billing_readiness
from apps.worker.durable_workflows import BillingFinalizationWorkflow


@pytest.mark.asyncio
async def test_billing_workflow_usage_signals_are_dedup_safe() -> None:
    workflow = BillingFinalizationWorkflow()

    await workflow.usage_recorded({"usage_type": "tts_characters"})
    await workflow.usage_recorded({"usage_type": "tts_characters"})

    status = workflow.status()
    assert status.present_usage_types == ["tts_characters"]
    assert status.state == BillingWorkflowState.WAITING_FOR_CALL_END


@pytest.mark.asyncio
async def test_billing_workflow_projection_signal_is_coalesced_hint() -> None:
    workflow = BillingFinalizationWorkflow()

    await workflow.usage_projection_updated({"call_id": "call-1"})
    await workflow.usage_projection_updated({"call_id": "call-1"})

    assert workflow.projection_update_count == 2
    status = workflow.status()
    assert status.manifest_present is False
    assert status.projection_caught_up is False


@pytest.mark.asyncio
async def test_billing_workflow_call_end_signal() -> None:
    workflow = BillingFinalizationWorkflow()

    await workflow.call_ended_signal({"call_id": "call-1"})

    assert workflow.call_ended is True


def test_decimal_cents_rounding() -> None:
    from decimal import Decimal

    assert cents(Decimal("0.034")) == 3
    assert cents(Decimal("0.035")) == 4


def test_billing_component_costs_prefers_stage_over_token_name() -> None:
    costs = _billing_component_costs(
        [
            {"stage": "tts", "usage_type": "input_text_token", "cost_usd": "0.036"},
            {"stage": "llm", "usage_type": "output_token", "cost_usd": "0.024"},
            {"stage": "stt", "usage_type": "audio_minute", "cost_usd": "0.051"},
        ]
    )

    assert costs["tts"] == 4
    assert costs["llm"] == 2
    assert costs["stt"] == 5


@pytest.mark.asyncio
async def test_billing_readiness_reports_projection_lag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_fetchrow(query: str, *args: object) -> dict[str, object] | None:
        if "call_usage_manifests" in query:
            return {
                "barrier_topic": "usage-events",
                "barrier_partition": 1,
                "barrier_offset": 42,
            }
        if "projection_watermarks" in query:
            return {"last_projected_offset": 41}
        return None

    async def fake_fetch(query: str, *args: object) -> list[dict[str, object]]:
        if "call_usage_expectations" in query:
            return []
        return [{"usage_type": "stt_audio_seconds"}]

    monkeypatch.setattr("apps.worker.activities._fetchrow", fake_fetchrow)
    monkeypatch.setattr("apps.worker.activities._fetch", fake_fetch)

    readiness = await load_billing_readiness(
        {
            "call_id": "call-1",
            "consumer_group": "voicemesh-postgres-projector-v1",
            "required_usage_types": ["stt_audio_seconds"],
        }
    )

    assert readiness["manifest_present"] is True
    assert readiness["projection_caught_up"] is False
    assert readiness["projection_watermark"] == 41


@pytest.mark.asyncio
async def test_billing_readiness_reports_missing_turn_expectations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_fetchrow(query: str, *args: object) -> dict[str, object] | None:
        if "call_usage_manifests" in query:
            return {
                "barrier_topic": "usage-events",
                "barrier_partition": 1,
                "barrier_offset": 42,
            }
        if "projection_watermarks" in query:
            return {"last_projected_offset": 42}
        return None

    async def fake_fetch(query: str, *args: object) -> list[dict[str, object]]:
        if "call_usage_expectations" in query:
            return [{"turn_id": "turn-2", "usage_type": "tts_characters"}]
        return [{"usage_type": "stt_audio_seconds"}]

    monkeypatch.setattr("apps.worker.activities._fetchrow", fake_fetchrow)
    monkeypatch.setattr("apps.worker.activities._fetch", fake_fetch)

    readiness = await load_billing_readiness(
        {
            "call_id": "call-1",
            "consumer_group": "voicemesh-postgres-projector-v1",
            "required_usage_types": ["stt_audio_seconds"],
        }
    )

    assert readiness["projection_caught_up"] is True
    assert readiness["missing_expectations"] == ["turn-2:tts_characters"]
