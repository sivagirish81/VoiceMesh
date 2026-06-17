import asyncio
import json
import sys
from pathlib import Path
from uuid import uuid4

import asyncpg
from temporalio.client import Client

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from apps.api.config import get_settings


def refund_tool_config() -> dict[str, object]:
    return {
        "tool_name": "refund_request",
        "mode": "DURABLE_ACTION",
        "create": {
            "method": "POST",
            "url": "http://api:8000/mock-customer/refund-requests",
            "idempotency_key_template": "{{tool_invocation_id}}:create",
            "timeout_seconds": 10,
        },
        "cancel": {
            "method": "POST",
            "url_template": (
                "http://api:8000/mock-customer/refund-requests/"
                "{{external_request_id}}/cancel"
            ),
            "idempotency_key_template": "{{tool_invocation_id}}:cancel",
            "timeout_seconds": 10,
        },
        "status": {
            "method": "GET",
            "url_template": (
                "http://api:8000/mock-customer/refund-requests/"
                "{{external_request_id}}"
            ),
            "timeout_seconds": 10,
        },
        "response_mapping": {
            "external_request_id": "$.refund_request_id",
            "status": "$.status",
            "message": "$.message",
        },
        "terminal_states": ["cancelled", "cannot_cancel", "failed", "refunded", "rejected"],
        "poll_interval_seconds": 2,
        "max_runtime_seconds": 30,
    }


async def main() -> None:
    settings = get_settings()
    call_id = f"durable-demo-{uuid4()}"
    tool_invocation_id = f"refund-{uuid4()}"
    workflow_id = f"tool-{tool_invocation_id}"
    client = await Client.connect(settings.temporal_address, namespace=settings.temporal_namespace)
    handle = await client.start_workflow(
        "DurableActionWorkflow",
        {
            "tenant_id": "local-demo-tenant",
            "assistant_id": "refund-agent",
            "call_id": call_id,
            "turn_id": "turn-1",
            "tool_invocation_id": tool_invocation_id,
            "tool_name": "refund_request",
            "tool_arguments": {
                "refund_request_id": "rr_001",
                "delay_seconds": 4,
                "amount_cents": 1200,
            },
            "tool_config": refund_tool_config(),
        },
        id=workflow_id,
        task_queue=settings.temporal_task_queue,
    )
    await asyncio.sleep(1)
    await handle.signal("CancelRequested", {"reason": "user_changed_mind"})
    result = await handle.result()
    connection = await asyncpg.connect(settings.database_url)
    try:
        row = await connection.fetchrow(
            "SELECT * FROM tool_invocations WHERE tool_invocation_id=$1",
            tool_invocation_id,
        )
        attempts = await connection.fetch(
            """
            SELECT activity_name, status_code, success, error
            FROM tool_invocation_attempts
            WHERE tool_invocation_id=$1
            ORDER BY attempt_id
            """,
            tool_invocation_id,
        )
    finally:
        await connection.close()
    print(
        json.dumps(
            {
                "workflow_id": workflow_id,
                "workflow_result": result,
                "tool_invocation": dict(row) if row else None,
                "attempts": [dict(attempt) for attempt in attempts],
            },
            default=str,
            indent=2,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
