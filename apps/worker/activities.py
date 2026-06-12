import json
from datetime import UTC, datetime
from typing import Any, cast
from uuid import uuid4

import asyncpg
from opentelemetry import trace
from temporalio import activity

from apps.api.config import get_settings

tracer = trace.get_tracer(__name__)


async def _execute(query: str, *args: Any) -> str:
    settings = get_settings()
    connection = await asyncpg.connect(settings.database_url)
    try:
        return cast(str, await connection.execute(query, *args))
    finally:
        await connection.close()


@activity.defn
async def persist_call_state(data: dict[str, Any]) -> None:
    with tracer.start_as_current_span("temporal.activity.persist_call_state"):
        await _execute(
            """
            UPDATE calls SET status=$2, updated_at=NOW()
            WHERE call_id=$1
            """,
            data["call_id"],
            data["state"],
        )


@activity.defn
async def select_fallback_provider(data: dict[str, Any]) -> dict[str, Any]:
    settings = get_settings()
    connection = await asyncpg.connect(settings.database_url)
    try:
        row = await connection.fetchrow(
            """
            SELECT provider_name
            FROM provider_configs
            WHERE provider_type=$1 AND enabled=TRUE AND provider_name <> $2
            ORDER BY id LIMIT 1
            """,
            data["stage"],
            data["provider"],
        )
        return {"fallback_provider": row["provider_name"] if row else None}
    finally:
        await connection.close()


@activity.defn
async def emit_recovery_event(data: dict[str, Any]) -> None:
    event_id = uuid4()
    payload = {
        "event_id": str(event_id),
        "call_id": data["call_id"],
        "turn_id": "session",
        "event_type": "workflow.state_changed",
        "stage": "temporal",
        "timestamp": datetime.now(UTC).isoformat(),
        "sequence_number": 1,
        "idempotency_key": f"{data['call_id']}:workflow:recovery",
        "payload": data,
        "trace_id": None,
    }
    await _execute(
        """
        INSERT INTO outbox_events (event_id, topic, key, payload)
        VALUES ($1, 'call-events', $2, $3::jsonb)
        ON CONFLICT (event_id) DO NOTHING
        """,
        event_id,
        data["call_id"],
        json.dumps(payload),
    )


@activity.defn
async def summarize_call(data: dict[str, Any]) -> str:
    return str(data.get("summary") or "Call completed without a generated summary.")


@activity.defn
async def mark_call_completed(data: dict[str, Any]) -> None:
    await _execute(
        """
        UPDATE calls SET status='CALL_COMPLETED', final_summary=$2,
        ended_at=COALESCE(ended_at, NOW()), updated_at=NOW() WHERE call_id=$1
        """,
        data["call_id"],
        data["summary"],
    )


@activity.defn
async def mark_call_failed(data: dict[str, Any]) -> None:
    await _execute(
        """
        UPDATE calls SET status='CALL_FAILED', error=$2,
        ended_at=COALESCE(ended_at, NOW()), updated_at=NOW() WHERE call_id=$1
        """,
        data["call_id"],
        data.get("error", "unknown workflow failure"),
    )
