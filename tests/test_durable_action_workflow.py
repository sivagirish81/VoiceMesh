import pytest

from apps.api.tools.http_config import (
    conversational_tool_response,
    extract_json_path,
    render_template,
)
from apps.api.tools.models import DurableActionState
from apps.worker.durable_workflows import DurableActionWorkflow


@pytest.mark.asyncio
async def test_cancel_before_external_id_is_recorded_idempotently() -> None:
    workflow = DurableActionWorkflow()

    await workflow.cancel_requested_signal({"reason": "user_changed_mind"})
    await workflow.cancel_requested_signal({"reason": "duplicate"})

    status = workflow.status()
    assert status.state == DurableActionState.REQUESTED
    assert status.cancel_requested is True
    assert workflow.cancel_reason == "duplicate"
    assert status.external_request_id is None


@pytest.mark.asyncio
async def test_external_status_signal_updates_queryable_state() -> None:
    workflow = DurableActionWorkflow()

    await workflow.external_status_updated({"status": "cancelled", "message": "done"})

    assert workflow.external_status == {"status": "cancelled", "message": "done"}


def test_tool_template_and_json_path_helpers() -> None:
    rendered = render_template(
        "https://customer.example.com/refunds/{{external_request_id}}/cancel",
        {"external_request_id": "rr_001"},
    )
    assert rendered.endswith("/rr_001/cancel")
    assert extract_json_path({"refund": {"status": "cancelled"}}, "$.refund.status") == "cancelled"


def test_conversational_tool_response_for_cancelled() -> None:
    assert conversational_tool_response("CANCELLED") == "No problem, I cancelled that request."
