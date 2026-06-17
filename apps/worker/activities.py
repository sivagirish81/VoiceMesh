import json
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, cast
from uuid import uuid4

import asyncpg
import httpx
from opentelemetry import trace
from temporalio import activity

from apps.api.config import get_settings
from apps.api.events.schemas import EventType, PipelineEvent
from apps.api.telemetry.tracing import context_from_payload, set_span_attributes
from apps.api.tools.http_config import cents, extract_json_path, render_template
from apps.api.tools.models import DurableToolConfig, ExternalHttpConfig

tracer = trace.get_tracer(__name__)


async def _execute(query: str, *args: Any) -> str:
    settings = get_settings()
    connection = await asyncpg.connect(settings.database_url)
    try:
        return cast(str, await connection.execute(query, *args))
    finally:
        await connection.close()


async def _fetchrow(query: str, *args: Any) -> asyncpg.Record | None:
    settings = get_settings()
    connection = await asyncpg.connect(settings.database_url)
    try:
        return await connection.fetchrow(query, *args)
    finally:
        await connection.close()


async def _fetch(query: str, *args: Any) -> list[asyncpg.Record]:
    settings = get_settings()
    connection = await asyncpg.connect(settings.database_url)
    try:
        return list(await connection.fetch(query, *args))
    finally:
        await connection.close()


async def _insert_outbox_event(event: PipelineEvent) -> None:
    await _execute(
        """
        INSERT INTO outbox_events (event_id, topic, key, payload)
        VALUES ($1, $2, $3, $4::jsonb)
        ON CONFLICT (event_id) DO NOTHING
        """,
        event.event_id,
        "tool-events" if str(event.event_type).startswith("tool.") else "billing-events"
        if str(event.event_type).startswith("billing.")
        else "webhook-events"
        if str(event.event_type).startswith("webhook.")
        else "call-events",
        event.call_id,
        event.model_dump_json(),
    )


def _context_values(data: dict[str, Any]) -> dict[str, Any]:
    return {
        **data,
        **data.get("tool_arguments", {}),
        "external_request_id": data.get("external_request_id"),
    }


def _billing_component_costs(usage: list[Mapping[str, Any]]) -> dict[str, int]:
    provider_costs = {
        "stt": Decimal("0"),
        "llm": Decimal("0"),
        "tts": Decimal("0"),
        "telephony": Decimal("0"),
    }
    for row in usage:
        stage = str(row["stage"])
        usage_type = str(row["usage_type"])
        cost_usd = Decimal(str(row["cost_usd"]))
        if stage == "stt" or usage_type.startswith("audio_"):
            provider_costs["stt"] += cost_usd
        elif stage == "tts" or usage_type.startswith("input_text"):
            provider_costs["tts"] += cost_usd
        elif stage == "llm" or "token" in usage_type:
            provider_costs["llm"] += cost_usd
        elif stage in {"transport", "telephony"} or "telephony" in usage_type:
            provider_costs["telephony"] += cost_usd
        else:
            provider_costs["llm"] += cost_usd
    return {component: cents(cost) for component, cost in provider_costs.items()}


async def _record_tool_attempt(
    *,
    tool_invocation_id: str,
    activity_name: str,
    request_url: str | None,
    status_code: int | None,
    success: bool,
    error: str | None,
) -> None:
    await _execute(
        """
        INSERT INTO tool_invocation_attempts (
            tool_invocation_id, activity_name, request_url, status_code,
            success, error, completed_at
        ) VALUES ($1,$2,$3,$4,$5,$6,NOW())
        """,
        tool_invocation_id,
        activity_name,
        request_url,
        status_code,
        success,
        error,
    )


@activity.defn
async def persist_call_state(data: dict[str, Any]) -> None:
    with tracer.start_as_current_span(
        "temporal.activity.persist_call_state",
        context=context_from_payload(data),
    ) as span:
        set_span_attributes(span, call_id=data["call_id"], workflow_state=data["state"])
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
    with tracer.start_as_current_span(
        "temporal.activity.select_fallback_provider",
        context=context_from_payload(data),
    ) as span:
        set_span_attributes(
            span,
            call_id=data["call_id"],
            stage=data["stage"],
            provider=data["provider"],
        )
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
            fallback = row["provider_name"] if row else None
            set_span_attributes(span, fallback_provider=fallback)
            return {"fallback_provider": fallback}
        finally:
            await connection.close()


@activity.defn
async def emit_recovery_event(data: dict[str, Any]) -> None:
    with tracer.start_as_current_span(
        "temporal.activity.emit_recovery_event",
        context=context_from_payload(data),
    ) as span:
        set_span_attributes(span, call_id=data["call_id"], reason=data.get("reason"))
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
    with tracer.start_as_current_span(
        "temporal.activity.summarize_call",
        context=context_from_payload(data),
    ) as span:
        set_span_attributes(
            span,
            call_id=data["call_id"],
            summary_chars=len(str(data.get("summary") or "")),
        )
        return str(data.get("summary") or "Call completed without a generated summary.")


@activity.defn
async def mark_call_completed(data: dict[str, Any]) -> None:
    with tracer.start_as_current_span(
        "temporal.activity.mark_call_completed",
        context=context_from_payload(data),
    ) as span:
        set_span_attributes(span, call_id=data["call_id"], summary_chars=len(data["summary"]))
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
    with tracer.start_as_current_span(
        "temporal.activity.mark_call_failed",
        context=context_from_payload(data),
    ) as span:
        error = data.get("error", "unknown workflow failure")
        set_span_attributes(span, call_id=data["call_id"], error_message=error)
        await _execute(
            """
            UPDATE calls SET status='CALL_FAILED', error=$2,
            ended_at=COALESCE(ended_at, NOW()), updated_at=NOW() WHERE call_id=$1
            """,
            data["call_id"],
            error,
        )


@activity.defn
async def persist_tool_state(data: dict[str, Any]) -> None:
    with tracer.start_as_current_span(
        "temporal.activity.persist_tool_state",
        context=context_from_payload(data),
    ) as span:
        set_span_attributes(
            span,
            workflow_id=data.get("workflow_id"),
            tenant_id=data.get("tenant_id"),
            assistant_id=data.get("assistant_id"),
            call_id=data["call_id"],
            tool_invocation_id=data["tool_invocation_id"],
            tool_name=data["tool_name"],
            status=data["state"],
        )
        terminal = data["state"] in {
            "CANCELLED",
            "COMPLETED",
            "CANNOT_CANCEL",
            "FAILED",
            "TIMED_OUT",
        }
        await _execute(
            """
            INSERT INTO tool_invocations (
                tool_invocation_id, tenant_id, assistant_id, call_id, turn_id,
                tool_name, execution_mode, workflow_id, external_request_id,
                status, arguments_json, result_json, last_error, cancel_requested,
                cancel_reason, completed_at
            ) VALUES ($1,$2,$3,$4,$5,$6,'DURABLE_ACTION',$7,$8,$9,$10::jsonb,
                $11::jsonb,$12,$13,$14,CASE WHEN $15 THEN NOW() ELSE NULL END)
            ON CONFLICT (tool_invocation_id) DO UPDATE SET
                workflow_id=COALESCE(EXCLUDED.workflow_id, tool_invocations.workflow_id),
                external_request_id=COALESCE(
                    EXCLUDED.external_request_id, tool_invocations.external_request_id
                ),
                status=EXCLUDED.status,
                result_json=EXCLUDED.result_json,
                last_error=EXCLUDED.last_error,
                cancel_requested=EXCLUDED.cancel_requested,
                cancel_reason=COALESCE(EXCLUDED.cancel_reason, tool_invocations.cancel_reason),
                completed_at=COALESCE(EXCLUDED.completed_at, tool_invocations.completed_at),
                updated_at=NOW()
            """,
            data["tool_invocation_id"],
            data.get("tenant_id", "local-demo-tenant"),
            data.get("assistant_id", "local-demo-assistant"),
            data["call_id"],
            data["turn_id"],
            data["tool_name"],
            data.get("workflow_id"),
            data.get("external_request_id"),
            data["state"],
            json.dumps(data.get("tool_arguments", {})),
            json.dumps(
                {
                    "message": data.get("message"),
                    "external_request_id": data.get("external_request_id"),
                }
            ),
            data.get("last_error"),
            bool(data.get("cancel_requested", False)),
            data.get("cancel_reason"),
            terminal,
        )


@activity.defn
async def emit_tool_event(data: dict[str, Any]) -> None:
    with tracer.start_as_current_span(
        "temporal.activity.emit_tool_event",
        context=context_from_payload(data),
    ) as span:
        event_type = EventType(str(data["event_type"]))
        set_span_attributes(
            span,
            workflow_id=data.get("workflow_id"),
            tenant_id=data.get("tenant_id"),
            assistant_id=data.get("assistant_id"),
            call_id=data["call_id"],
            tool_invocation_id=data["tool_invocation_id"],
            event_type=str(event_type),
            status=data.get("state"),
        )
        event = PipelineEvent.create(
            call_id=data["call_id"],
            turn_id=data["turn_id"],
            event_type=event_type,
            stage="tool",
            sequence_number=1,
            idempotency_key=f"{data['tool_invocation_id']}:{event_type}:{data.get('state')}",
            payload={
                "event_version": 1,
                "tenant_id": data.get("tenant_id", "local-demo-tenant"),
                "assistant_id": data.get("assistant_id", "local-demo-assistant"),
                "tool_invocation_id": data["tool_invocation_id"],
                "workflow_id": data.get("workflow_id"),
                "tool_name": data["tool_name"],
                "state": data.get("state"),
                "external_request_id": data.get("external_request_id"),
                "message": data.get("message"),
                "last_error": data.get("last_error"),
            },
            trace_id=data.get("trace_id"),
        )
        await _insert_outbox_event(event)


async def _call_external_action(
    data: dict[str, Any],
    *,
    action: str,
) -> dict[str, Any]:
    config = DurableToolConfig.model_validate(data["tool_config"])
    http_config: ExternalHttpConfig | None
    if action == "create":
        http_config = config.create
    elif action == "cancel":
        http_config = config.cancel
    else:
        http_config = config.status
    if not http_config:
        raise ValueError(f"No {action} HTTP config defined for {config.tool_name}")
    values = _context_values(data)
    url = http_config.url or render_template(http_config.url_template or "", values)
    idem_template = http_config.idempotency_key_template or f"{{{{tool_invocation_id}}}}:{action}"
    idempotency_key = render_template(idem_template, values)
    headers = {"Idempotency-Key": idempotency_key}
    body = data.get("tool_arguments", {}) if action != "status" else None
    status_code: int | None = None
    try:
        async with httpx.AsyncClient(timeout=http_config.timeout_seconds) as client:
            response = await client.request(
                http_config.method,
                url,
                json=body,
                headers=headers,
            )
            status_code = response.status_code
            response.raise_for_status()
            payload = response.json() if response.content else {}
        await _record_tool_attempt(
            tool_invocation_id=data["tool_invocation_id"],
            activity_name=f"{action}_external_action",
            request_url=url,
            status_code=status_code,
            success=True,
            error=None,
        )
        mapping = config.response_mapping
        external_request_id = extract_json_path(payload, mapping.external_request_id)
        return {
            "external_request_id": external_request_id or data.get("external_request_id"),
            "status": extract_json_path(payload, mapping.status),
            "message": extract_json_path(payload, mapping.message),
            "raw": payload,
        }
    except Exception as exc:
        await _record_tool_attempt(
            tool_invocation_id=data["tool_invocation_id"],
            activity_name=f"{action}_external_action",
            request_url=url,
            status_code=status_code,
            success=False,
            error=str(exc),
        )
        raise


@activity.defn
async def create_external_action(data: dict[str, Any]) -> dict[str, Any]:
    with tracer.start_as_current_span(
        "temporal.activity.create_external_action",
        context=context_from_payload(data),
    ) as span:
        set_span_attributes(
            span,
            workflow_id=data.get("workflow_id"),
            tenant_id=data.get("tenant_id"),
            assistant_id=data.get("assistant_id"),
            call_id=data["call_id"],
            tool_invocation_id=data["tool_invocation_id"],
            activity_name="create_external_action",
        )
        return await _call_external_action(data, action="create")


@activity.defn
async def cancel_external_action(data: dict[str, Any]) -> dict[str, Any]:
    with tracer.start_as_current_span(
        "temporal.activity.cancel_external_action",
        context=context_from_payload(data),
    ) as span:
        set_span_attributes(
            span,
            workflow_id=data.get("workflow_id"),
            tenant_id=data.get("tenant_id"),
            assistant_id=data.get("assistant_id"),
            call_id=data["call_id"],
            tool_invocation_id=data["tool_invocation_id"],
            external_request_id=data.get("external_request_id"),
            activity_name="cancel_external_action",
        )
        return await _call_external_action(data, action="cancel")


@activity.defn
async def get_external_action_status(data: dict[str, Any]) -> dict[str, Any]:
    with tracer.start_as_current_span(
        "temporal.activity.get_external_action_status",
        context=context_from_payload(data),
    ) as span:
        set_span_attributes(
            span,
            workflow_id=data.get("workflow_id"),
            tenant_id=data.get("tenant_id"),
            assistant_id=data.get("assistant_id"),
            call_id=data["call_id"],
            tool_invocation_id=data["tool_invocation_id"],
            external_request_id=data.get("external_request_id"),
            activity_name="get_external_action_status",
        )
        return await _call_external_action(data, action="status")


@activity.defn
async def load_billing_readiness(data: dict[str, Any]) -> dict[str, Any]:
    with tracer.start_as_current_span(
        "temporal.activity.load_billing_readiness",
        context=context_from_payload(data),
    ) as span:
        rows = await _fetch(
            """
            SELECT usage_type FROM call_usage_events WHERE call_id=$1
            UNION
            SELECT CASE usage_type
                WHEN 'audio_minute' THEN 'stt_audio_seconds'
                WHEN 'input_token' THEN 'llm_input_tokens'
                WHEN 'cached_input_token' THEN 'llm_input_tokens'
                WHEN 'output_token' THEN 'llm_output_tokens'
                WHEN 'output_audio_token' THEN 'tts_audio_seconds'
                WHEN 'input_text_token' THEN 'tts_characters'
                ELSE usage_type
            END AS usage_type
            FROM usage_records WHERE call_id=$1
            """,
            data["call_id"],
        )
        present = {str(row["usage_type"]) for row in rows}
        required = set(data.get("required_usage_types", []))
        missing = sorted(required - present)
        set_span_attributes(
            span,
            call_id=data["call_id"],
            billing_status="WAITING_FOR_USAGE" if missing else "READY",
            present_usage_types=sorted(present),
            missing_usage_types=missing,
        )
        return {
            "present_usage_types": sorted(present),
            "missing_usage_types": missing,
        }


@activity.defn
async def finalize_call_billing(data: dict[str, Any]) -> dict[str, Any]:
    with tracer.start_as_current_span(
        "temporal.activity.finalize_call_billing",
        context=context_from_payload(data),
    ) as span:
        call = await _fetchrow("SELECT * FROM calls WHERE call_id=$1", data["call_id"])
        if not call:
            raise RuntimeError(f"Call {data['call_id']} not found")
        usage = await _fetch("SELECT * FROM usage_records WHERE call_id=$1", data["call_id"])
        billing = await _fetchrow("SELECT * FROM call_billing WHERE call_id=$1", data["call_id"])
        duration_seconds = Decimal("0")
        if billing and billing["call_duration_seconds"]:
            duration_seconds = Decimal(str(billing["call_duration_seconds"]))
        elif call["started_at"] and call["ended_at"]:
            duration_seconds = Decimal(str((call["ended_at"] - call["started_at"]).total_seconds()))
        platform_cost_cents = cents((duration_seconds / Decimal("60")) * Decimal("0.05"))
        component_costs = _billing_component_costs(usage)
        stt_cost_cents = component_costs["stt"]
        llm_cost_cents = component_costs["llm"]
        tts_cost_cents = component_costs["tts"]
        telephony_cost_cents = component_costs["telephony"]
        total_cost_cents = (
            platform_cost_cents
            + stt_cost_cents
            + llm_cost_cents
            + tts_cost_cents
            + telephony_cost_cents
        )
        warnings = [
            f"missing usage: {usage_type}" for usage_type in data.get("missing_usage_types", [])
        ]
        status = str(data["status"])
        await _execute(
            """
            INSERT INTO final_call_billing_records (
                call_id, tenant_id, assistant_id, started_at, ended_at, billable_seconds,
                platform_cost_cents, stt_cost_cents, llm_cost_cents, tts_cost_cents,
                telephony_cost_cents, total_cost_cents, pricing_version, status,
                warnings, finalized_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb,NOW())
            ON CONFLICT (call_id) DO UPDATE SET
                billable_seconds=EXCLUDED.billable_seconds,
                platform_cost_cents=EXCLUDED.platform_cost_cents,
                stt_cost_cents=EXCLUDED.stt_cost_cents,
                llm_cost_cents=EXCLUDED.llm_cost_cents,
                tts_cost_cents=EXCLUDED.tts_cost_cents,
                total_cost_cents=EXCLUDED.total_cost_cents,
                status=EXCLUDED.status,
                warnings=EXCLUDED.warnings,
                finalized_at=COALESCE(final_call_billing_records.finalized_at, NOW()),
                updated_at=NOW()
            """,
            data["call_id"],
            data.get("tenant_id", "local-demo-tenant"),
            data.get("assistant_id", "local-demo-assistant"),
            call["started_at"],
            call["ended_at"],
            int(duration_seconds.to_integral_value(rounding=ROUND_HALF_UP)),
            platform_cost_cents,
            stt_cost_cents,
            llm_cost_cents,
            tts_cost_cents,
            telephony_cost_cents,
            total_cost_cents,
            data.get("pricing_version", "local-demo-v1"),
            status,
            json.dumps(warnings),
        )
        set_span_attributes(
            span,
            call_id=data["call_id"],
            billing_status=status,
            total_cost_cents=total_cost_cents,
        )
        return {
            "call_id": data["call_id"],
            "status": status,
            "platform_cost_cents": platform_cost_cents,
            "stt_cost_cents": stt_cost_cents,
            "llm_cost_cents": llm_cost_cents,
            "tts_cost_cents": tts_cost_cents,
            "telephony_cost_cents": telephony_cost_cents,
            "total_cost_cents": total_cost_cents,
            "warnings": warnings,
        }


@activity.defn
async def emit_workflow_event(data: dict[str, Any]) -> None:
    with tracer.start_as_current_span(
        "temporal.activity.emit_workflow_event",
        context=context_from_payload(data),
    ) as span:
        event_type = EventType(str(data["event_type"]))
        set_span_attributes(
            span,
            call_id=data["call_id"],
            workflow_id=data.get("workflow_id"),
            event_type=str(event_type),
        )
        event = PipelineEvent.create(
            call_id=data["call_id"],
            turn_id="session",
            event_type=event_type,
            stage="temporal",
            sequence_number=1,
            idempotency_key=f"{data['call_id']}:{data['event_type']}:{data.get('workflow_id')}",
            payload={
                "event_version": 1,
                "tenant_id": data.get("tenant_id", "local-demo-tenant"),
                "assistant_id": data.get("assistant_id", "local-demo-assistant"),
                "workflow_id": data.get("workflow_id"),
                **dict(data.get("payload", {})),
            },
            trace_id=data.get("trace_id"),
        )
        await _insert_outbox_event(event)


@activity.defn
async def persist_webhook_delivery(data: dict[str, Any]) -> None:
    await _execute(
        """
        INSERT INTO webhook_deliveries (
            webhook_delivery_id, tenant_id, assistant_id, call_id, workflow_id,
            target_url, event_type, payload, status, attempts, last_status_code,
            last_error, idempotency_key, completed_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,COALESCE($10, 0),$11,$12,$13,
            CASE WHEN $9 IN ('DELIVERED','FAILED') THEN NOW() ELSE NULL END)
        ON CONFLICT (webhook_delivery_id) DO UPDATE SET
            workflow_id=COALESCE(EXCLUDED.workflow_id, webhook_deliveries.workflow_id),
            status=EXCLUDED.status,
            attempts=GREATEST(webhook_deliveries.attempts, EXCLUDED.attempts),
            last_status_code=COALESCE(
                EXCLUDED.last_status_code,
                webhook_deliveries.last_status_code
            ),
            last_error=COALESCE(EXCLUDED.last_error, webhook_deliveries.last_error),
            completed_at=COALESCE(EXCLUDED.completed_at, webhook_deliveries.completed_at),
            updated_at=NOW()
        """,
        data["webhook_delivery_id"],
        data.get("tenant_id", "local-demo-tenant"),
        data.get("assistant_id", "local-demo-assistant"),
        data["call_id"],
        data.get("workflow_id"),
        data["target_url"],
        data.get("event_type", "end_of_call_report"),
        json.dumps(data.get("payload", {})),
        data["status"],
        data.get("attempts"),
        data.get("last_status_code"),
        data.get("last_error"),
        data["idempotency_key"],
    )


@activity.defn
async def deliver_webhook(data: dict[str, Any]) -> dict[str, Any]:
    attempt = int(data["attempt_number"])
    status_code: int | None = None
    error: str | None = None
    success = False
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(
                data["target_url"],
                json=data.get("payload", {}),
                headers={"Idempotency-Key": data["idempotency_key"]},
            )
            status_code = response.status_code
            success = 200 <= response.status_code < 300
            if not success:
                error = response.text[:500]
    except Exception as exc:
        error = str(exc)
    await _execute(
        """
        INSERT INTO webhook_delivery_attempts (
            webhook_delivery_id, attempt_number, status_code, success, error, completed_at
        ) VALUES ($1,$2,$3,$4,$5,NOW())
        """,
        data["webhook_delivery_id"],
        attempt,
        status_code,
        success,
        error,
    )
    await persist_webhook_delivery(
        {
            **data,
            "status": "DELIVERED" if success else "DELIVERING",
            "attempts": attempt,
            "last_status_code": status_code,
            "last_error": error,
        }
    )
    return {"success": success, "status_code": status_code, "error": error}
