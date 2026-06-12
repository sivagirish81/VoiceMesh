import pytest

from apps.worker.workflows import CallState, CallWorkflow


@pytest.mark.asyncio
async def test_workflow_signals_are_high_level_and_ordered() -> None:
    workflow = CallWorkflow()
    await workflow.call_started({"call_id": "call-1"})
    await workflow.pipeline_event({"event_type": "pipeline.corked"})
    await workflow.call_completed({"summary": "done"})
    assert [event.kind for event in workflow.events] == [
        "call_started",
        "pipeline_event",
        "call_completed",
    ]
    assert workflow.state == CallState.PENDING

