import pytest

from apps.api.tools.http_config import cents
from apps.api.tools.models import BillingWorkflowState
from apps.worker.activities import _billing_component_costs
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
