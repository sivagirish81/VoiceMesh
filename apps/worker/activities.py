import json
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, cast
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import asyncpg
import httpx
from opentelemetry import trace
from temporalio import activity

from apps.api.config import get_settings
from apps.api.events.schemas import EventType, PipelineEvent
from apps.api.telemetry.metrics import (
    BILLING_ADJUSTMENTS_TOTAL,
    BILLING_FINALIZATION_DURATION,
    BILLING_WORKFLOWS_WAITING,
    TEMPORAL_ACTIVITIES_TOTAL,
    TEMPORAL_ACTIVITY_DURATION,
    TEMPORAL_WORKFLOWS_TOTAL,
    WEBHOOK_DELIVERIES_TOTAL,
    WEBHOOK_DELIVERY_ATTEMPTS_TOTAL,
    WEBHOOK_DELIVERY_DURATION,
)
from apps.api.telemetry.tracing import context_from_payload, set_span_attributes
from apps.api.tools.http_config import cents, extract_json_path, render_template
from apps.api.tools.models import DurableToolConfig, ExternalHttpConfig

tracer = trace.get_tracer(__name__)


def _activity_success(activity_name: str, started: float) -> None:
    TEMPORAL_ACTIVITIES_TOTAL.labels(activity_name, "completed").inc()
    TEMPORAL_ACTIVITY_DURATION.labels(activity_name).observe(time.perf_counter() - started)


def _activity_failure(activity_name: str, started: float) -> None:
    TEMPORAL_ACTIVITIES_TOTAL.labels(activity_name, "failed").inc()
    TEMPORAL_ACTIVITY_DURATION.labels(activity_name).observe(time.perf_counter() - started)


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
    activity_name = "persist_tool_state"
    started = time.perf_counter()
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
        _activity_success(activity_name, started)
        if terminal:
            TEMPORAL_WORKFLOWS_TOTAL.labels("DurableActionWorkflow", data["state"]).inc()


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
    activity_name = "create_external_action"
    started = time.perf_counter()
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
        try:
            result = await _call_external_action(data, action="create")
            _activity_success(activity_name, started)
            return result
        except Exception:
            _activity_failure(activity_name, started)
            raise


@activity.defn
async def cancel_external_action(data: dict[str, Any]) -> dict[str, Any]:
    activity_name = "cancel_external_action"
    started = time.perf_counter()
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
        try:
            result = await _call_external_action(data, action="cancel")
            _activity_success(activity_name, started)
            return result
        except Exception:
            _activity_failure(activity_name, started)
            raise


@activity.defn
async def get_external_action_status(data: dict[str, Any]) -> dict[str, Any]:
    activity_name = "get_external_action_status"
    started = time.perf_counter()
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
        try:
            result = await _call_external_action(data, action="status")
            _activity_success(activity_name, started)
            return result
        except Exception:
            _activity_failure(activity_name, started)
            raise


@activity.defn
async def load_billing_readiness(data: dict[str, Any]) -> dict[str, Any]:
    activity_name = "load_billing_readiness"
    started = time.perf_counter()
    with tracer.start_as_current_span(
        "temporal.activity.load_billing_readiness",
        context=context_from_payload(data),
    ) as span:
        manifest = await _fetchrow(
            """
            SELECT * FROM call_usage_manifests WHERE call_id=$1
            """,
            data["call_id"],
        )
        projection_caught_up = False
        watermark: int | None = None
        if manifest:
            watermark_row = await _fetchrow(
                """
                SELECT last_projected_offset FROM projection_watermarks
                WHERE consumer_group=$1 AND topic=$2 AND partition=$3
                """,
                data.get("consumer_group", "voicemesh-postgres-projector-v1"),
                manifest["barrier_topic"],
                manifest["barrier_partition"],
            )
            if watermark_row:
                watermark = int(watermark_row["last_projected_offset"])
                projection_caught_up = watermark >= int(manifest["barrier_offset"])
        rows = await _fetch(
            """
            SELECT e.turn_id, e.usage_type
            FROM call_usage_expectations e
            LEFT JOIN call_usage_events u
                ON u.call_id=e.call_id
                AND u.turn_id=e.turn_id
                AND u.usage_type=e.usage_type
            WHERE e.call_id=$1 AND u.event_id IS NULL
            ORDER BY e.turn_id, e.usage_type
            """,
            data["call_id"],
        )
        missing_expectations = [
            f"{row['turn_id']}:{row['usage_type']}" for row in rows
        ]
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
        final_billing = await _fetchrow(
            "SELECT status,total_cost_cents FROM final_call_billing_records WHERE call_id=$1",
            data["call_id"],
        )
        set_span_attributes(
            span,
            call_id=data["call_id"],
            billing_status="READY"
            if manifest and projection_caught_up and not missing_expectations and not missing
            else "WAITING",
            manifest_present=bool(manifest),
            projection_caught_up=projection_caught_up,
            projection_watermark=watermark,
            present_usage_types=sorted(present),
            missing_usage_types=missing,
            missing_expectations=missing_expectations,
        )
        if not manifest:
            BILLING_WORKFLOWS_WAITING.labels("WAITING_FOR_MANIFEST").inc()
        elif not projection_caught_up:
            BILLING_WORKFLOWS_WAITING.labels("WAITING_FOR_PROJECTION").inc()
        elif missing or missing_expectations:
            BILLING_WORKFLOWS_WAITING.labels("WAITING_FOR_USAGE").inc()
        _activity_success(activity_name, started)
        return {
            "manifest_present": bool(manifest),
            "barrier_topic": manifest["barrier_topic"] if manifest else None,
            "barrier_partition": manifest["barrier_partition"] if manifest else None,
            "barrier_offset": manifest["barrier_offset"] if manifest else None,
            "projection_watermark": watermark,
            "projection_caught_up": projection_caught_up,
            "present_usage_types": sorted(present),
            "missing_usage_types": missing,
            "missing_expectations": missing_expectations,
            "final_billing_status": final_billing["status"] if final_billing else None,
            "final_total_cost_cents": final_billing["total_cost_cents"] if final_billing else None,
        }


@activity.defn
async def finalize_call_billing(data: dict[str, Any]) -> dict[str, Any]:
    activity_name = "finalize_call_billing"
    started = time.perf_counter()
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
        warnings.extend(
            f"missing expected turn usage: {item}"
            for item in data.get("missing_expectations", [])
        )
        if data.get("manifest_missing"):
            warnings.append("missing usage finalization barrier")
        if not data.get("projection_caught_up", True):
            warnings.append("usage projection watermark did not catch up before timeout")
        status = str(data["status"])
        await _execute(
            """
            INSERT INTO final_call_billing_records (
                call_id, tenant_id, assistant_id, started_at, ended_at,
                connected_seconds, billable_seconds,
                platform_cost_cents, stt_cost_cents, llm_cost_cents, tts_cost_cents,
                telephony_cost_cents, total_cost_cents,
                platform_cost_microunits, stt_cost_microunits, llm_cost_microunits,
                tts_cost_microunits, telephony_cost_microunits,
                provider_cost_total_microunits, customer_charge_total_microunits,
                gross_margin_microunits, pricing_version, status, billing_status,
                expected_usage_components, received_usage_components,
                missing_usage_components, warnings, finalized_at, trace_id
            ) VALUES (
                $1,$2,$3,$4,$5,$6,$6,$7,$8,$9,$10,$11,$12,
                $13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$22,
                $23::jsonb,$24::jsonb,$25::jsonb,$26::jsonb,NOW(),$27
            )
            ON CONFLICT (call_id) DO UPDATE SET
                connected_seconds=EXCLUDED.connected_seconds,
                billable_seconds=EXCLUDED.billable_seconds,
                platform_cost_cents=EXCLUDED.platform_cost_cents,
                stt_cost_cents=EXCLUDED.stt_cost_cents,
                llm_cost_cents=EXCLUDED.llm_cost_cents,
                tts_cost_cents=EXCLUDED.tts_cost_cents,
                telephony_cost_cents=EXCLUDED.telephony_cost_cents,
                total_cost_cents=EXCLUDED.total_cost_cents,
                platform_cost_microunits=EXCLUDED.platform_cost_microunits,
                stt_cost_microunits=EXCLUDED.stt_cost_microunits,
                llm_cost_microunits=EXCLUDED.llm_cost_microunits,
                tts_cost_microunits=EXCLUDED.tts_cost_microunits,
                telephony_cost_microunits=EXCLUDED.telephony_cost_microunits,
                provider_cost_total_microunits=EXCLUDED.provider_cost_total_microunits,
                customer_charge_total_microunits=EXCLUDED.customer_charge_total_microunits,
                gross_margin_microunits=EXCLUDED.gross_margin_microunits,
                status=EXCLUDED.status,
                billing_status=EXCLUDED.billing_status,
                expected_usage_components=EXCLUDED.expected_usage_components,
                received_usage_components=EXCLUDED.received_usage_components,
                missing_usage_components=EXCLUDED.missing_usage_components,
                warnings=EXCLUDED.warnings,
                finalized_at=COALESCE(final_call_billing_records.finalized_at, NOW()),
                updated_at=NOW(),
                version=final_call_billing_records.version + 1,
                trace_id=COALESCE(EXCLUDED.trace_id, final_call_billing_records.trace_id)
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
            platform_cost_cents * 10000,
            stt_cost_cents * 10000,
            llm_cost_cents * 10000,
            tts_cost_cents * 10000,
            telephony_cost_cents * 10000,
            (stt_cost_cents + llm_cost_cents + tts_cost_cents + telephony_cost_cents) * 10000,
            total_cost_cents * 10000,
            (
                total_cost_cents
                - stt_cost_cents
                - llm_cost_cents
                - tts_cost_cents
                - telephony_cost_cents
            )
            * 10000,
            data.get("pricing_version", "local-demo-v1"),
            status,
            json.dumps(data.get("required_usage_types", [])),
            json.dumps(data.get("present_usage_types", [])),
            json.dumps(data.get("missing_usage_types", [])),
            json.dumps(warnings),
            data.get("trace_id"),
        )
        set_span_attributes(
            span,
            call_id=data["call_id"],
            billing_status=status,
            total_cost_cents=total_cost_cents,
        )
        _activity_success(activity_name, started)
        BILLING_FINALIZATION_DURATION.labels(status).observe(time.perf_counter() - started)
        TEMPORAL_WORKFLOWS_TOTAL.labels("BillingFinalizationWorkflow", status).inc()
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
async def create_billing_adjustment(data: dict[str, Any]) -> dict[str, Any]:
    activity_name = "create_billing_adjustment"
    started = time.perf_counter()
    with tracer.start_as_current_span(
        "temporal.activity.create_billing_adjustment",
        context=context_from_payload(data),
    ) as span:
        call = await _fetchrow("SELECT * FROM calls WHERE call_id=$1", data["call_id"])
        final = await _fetchrow(
            "SELECT * FROM final_call_billing_records WHERE call_id=$1",
            data["call_id"],
        )
        if not call or not final:
            raise RuntimeError(f"Final billing record is not available for {data['call_id']}")
        usage = await _fetch("SELECT * FROM usage_records WHERE call_id=$1", data["call_id"])
        billing = await _fetchrow("SELECT * FROM call_billing WHERE call_id=$1", data["call_id"])
        duration_seconds = Decimal("0")
        if billing and billing["call_duration_seconds"]:
            duration_seconds = Decimal(str(billing["call_duration_seconds"]))
        elif call["started_at"] and call["ended_at"]:
            duration_seconds = Decimal(str((call["ended_at"] - call["started_at"]).total_seconds()))
        platform_cost_cents = cents((duration_seconds / Decimal("60")) * Decimal("0.05"))
        component_costs = _billing_component_costs(usage)
        recomputed_total = (
            platform_cost_cents
            + component_costs["stt"]
            + component_costs["llm"]
            + component_costs["tts"]
            + component_costs["telephony"]
        )
        previous_total = int(final["total_cost_cents"])
        delta = recomputed_total - previous_total
        source_event_id = data.get("source_event_id")
        source_event_uuid = UUID(str(source_event_id)) if source_event_id else None
        adjustment_id = uuid5(
            NAMESPACE_URL,
            f"voicemesh:billing-adjustment:{data['call_id']}:{source_event_id}",
        )
        await _execute(
            """
            INSERT INTO billing_adjustments (
                adjustment_id, original_line_item_id, call_id, tenant_id, assistant_id,
                component, reason_code, provider_cost_delta_microunits,
                customer_charge_delta_microunits, currency, pricing_version,
                previous_total_cost_cents, recomputed_total_cost_cents,
                delta_cost_cents, reason, source_event_id, workflow_id, status,
                trace_id, idempotency_key
            ) VALUES (
                $1,NULL,$2,$3,$4,'billing',$5,$6,$6,'USD',$7,
                $8,$9,$10,$5,$11,$12,'CREATED',$13,$14
            )
            ON CONFLICT (call_id, source_event_id) DO NOTHING
            """,
            adjustment_id,
            data["call_id"],
            data.get("tenant_id", final["tenant_id"]),
            data.get("assistant_id", final["assistant_id"]),
            data.get("reason", "late_usage_after_finalization"),
            delta * 10000,
            data.get("pricing_version", final["pricing_version"]),
            previous_total,
            recomputed_total,
            delta,
            source_event_uuid,
            data.get("workflow_id"),
            data.get("trace_id"),
            f"billing-adjustment:{data['call_id']}:{source_event_id}",
        )
        set_span_attributes(
            span,
            call_id=data["call_id"],
            previous_total_cost_cents=previous_total,
            recomputed_total_cost_cents=recomputed_total,
            delta_cost_cents=delta,
        )
        _activity_success(activity_name, started)
        BILLING_ADJUSTMENTS_TOTAL.labels(
            data.get("reason", "late_usage_after_finalization")
        ).inc()
        TEMPORAL_WORKFLOWS_TOTAL.labels("BillingAdjustmentWorkflow", "CREATED").inc()
        return {
            "adjustment_id": str(adjustment_id),
            "call_id": data["call_id"],
            "previous_total_cost_cents": previous_total,
            "recomputed_total_cost_cents": recomputed_total,
            "delta_cost_cents": delta,
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
    WEBHOOK_DELIVERIES_TOTAL.labels(str(data["status"])).inc()
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
    activity_name = "deliver_webhook"
    started = time.perf_counter()
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
    status = "delivered" if success else "failed"
    WEBHOOK_DELIVERY_ATTEMPTS_TOTAL.labels(status).inc()
    WEBHOOK_DELIVERY_DURATION.labels(status).observe(time.perf_counter() - started)
    if success:
        _activity_success(activity_name, started)
    else:
        TEMPORAL_ACTIVITIES_TOTAL.labels(activity_name, "failed").inc()
        TEMPORAL_ACTIVITY_DURATION.labels(activity_name).observe(time.perf_counter() - started)
    return {"success": success, "status_code": status_code, "error": error}
