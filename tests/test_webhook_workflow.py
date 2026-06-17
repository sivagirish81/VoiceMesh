from apps.api.tools.models import WebhookDeliveryState
from apps.worker.durable_workflows import WebhookDeliveryWorkflow


def test_webhook_workflow_initial_query_state() -> None:
    workflow = WebhookDeliveryWorkflow()

    status = workflow.status()

    assert status["state"] == str(WebhookDeliveryState.REQUESTED)
    assert status["attempt_count"] == 0
