import json
import re
from datetime import UTC
from typing import Any

from apps.api.analytics.clickhouse.models import AnalyticsEventRow
from apps.api.events.kafka_consumer import ConsumedEvent
from apps.api.events.schemas import EventType, PipelineEvent

DEFAULT_TENANT_ID = "local-demo-tenant"
DEFAULT_ASSISTANT_ID = "local-demo-assistant"

_REDACTED_PAYLOAD_KEYS = {
    "audio",
    "audio_b64",
    "audio_bytes",
    "chunk",
    "data",
    "delta",
    "final_response",
    "generated_text",
    "spoken_text",
    "text",
    "transcript",
}


class AnalyticsNormalizationError(ValueError):
    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


def normalize_consumed_event(consumed: ConsumedEvent) -> AnalyticsEventRow:
    return normalize_pipeline_event(consumed.event)


def normalize_pipeline_event(event: PipelineEvent) -> AnalyticsEventRow:
    payload = event.payload or {}
    event_time = event.timestamp.astimezone(UTC)
    event_type = str(event.event_type)
    stage = str(payload.get("stage") or event.stage or "")
    provider = _string(payload.get("provider"))
    model = _string(payload.get("model"))
    status = _status_for(event.event_type, payload)
    reason_code = _reason_code(payload)
    latency_ms = _number(payload.get("latency_ms") or payload.get("first_token_latency_ms"))
    duration_ms = _duration_ms(event.event_type, payload)
    quantity, unit = _usage_quantity_unit(payload)

    return AnalyticsEventRow(
        event_id=str(event.event_id),
        event_type=event_type,
        event_version=int(payload.get("event_version") or 1),
        event_time=event_time,
        tenant_id=_string(payload.get("tenant_id") or DEFAULT_TENANT_ID),
        assistant_id=_string(payload.get("assistant_id") or DEFAULT_ASSISTANT_ID),
        call_id=event.call_id,
        turn_id=_string(payload.get("turn_id") or event.turn_id),
        response_id=_string(payload.get("response_id")),
        sequence=int(payload.get("sequence") or event.sequence_number or 0),
        stage=stage,
        provider=provider,
        model=model,
        status=status,
        reason_code=reason_code,
        latency_ms=latency_ms,
        duration_ms=duration_ms,
        quantity=quantity,
        unit=unit,
        trace_id=event.trace_id or _string(payload.get("trace_id")),
        idempotency_key=event.idempotency_key,
        payload_json=_payload_json(payload),
    )


def _string(value: Any) -> str:
    return "" if value is None else str(value)


def _number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise AnalyticsNormalizationError(
            "invalid_number",
            "payload contains invalid number",
        ) from exc


def _duration_ms(event_type: EventType, payload: dict[str, Any]) -> float | None:
    duration = _number(payload.get("duration_ms"))
    if duration is not None:
        return duration
    duration_seconds = _number(payload.get("duration_seconds"))
    if duration_seconds is not None:
        return duration_seconds * 1000
    if event_type == EventType.CALL_ENDED:
        call_duration = _number(payload.get("call_duration_seconds"))
        if call_duration is not None:
            return call_duration * 1000
    return None


def _usage_quantity_unit(payload: dict[str, Any]) -> tuple[float | None, str]:
    quantity = _number(payload.get("quantity"))
    unit = _string(payload.get("unit"))
    if quantity is not None:
        return quantity, unit

    measurements = payload.get("measurements")
    if not isinstance(measurements, list) or not measurements:
        return None, unit
    first = measurements[0]
    if not isinstance(first, dict):
        return None, unit
    return _number(first.get("quantity")), _string(first.get("unit"))


def _status_for(event_type: EventType, payload: dict[str, Any]) -> str:
    explicit = payload.get("status")
    if explicit:
        return _stable_code(str(explicit))
    mapping = {
        EventType.CALL_STARTED: "started",
        EventType.CALL_ENDED: "completed",
        EventType.CALL_FAILED: "failed",
        EventType.BILLING_FINALIZED: "finalized",
        EventType.PIPELINE_CORKED: "corked",
        EventType.PIPELINE_UNCORKED: "uncorked",
        EventType.PIPELINE_RESPONSE_CANCELLED: "cancelled",
        EventType.PIPELINE_STALE_CHUNK_DROPPED: "dropped",
        EventType.USER_BARGE_IN_CANDIDATE: "candidate",
        EventType.USER_BARGE_IN_CONFIRMED: "confirmed",
        EventType.USER_BARGE_IN_REJECTED: "rejected",
        EventType.VAD_NOISE_TURN_IGNORED: "ignored",
        EventType.PROVIDER_FAILED: "failed",
        EventType.WEBHOOK_DELIVERED: "delivered",
        EventType.WEBHOOK_FAILED: "failed",
        EventType.WORKFLOW_DONE: "done",
    }
    return mapping.get(event_type, "")


def _reason_code(payload: dict[str, Any]) -> str:
    for key in ("reason_code", "reason", "error_type", "category"):
        value = payload.get(key)
        if value:
            return _stable_code(str(value))
    error = payload.get("error")
    if error:
        return _stable_error_code(str(error))
    return ""


def _stable_error_code(message: str) -> str:
    lowered = message.lower()
    if "timeout" in lowered:
        return "timeout"
    if "rate" in lowered and "limit" in lowered:
        return "rate_limited"
    if "auth" in lowered or "api key" in lowered or "credential" in lowered:
        return "auth_failed"
    if "connect" in lowered or "network" in lowered:
        return "network_error"
    return "provider_error"


def _stable_code(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")
    return normalized[:80] or "unknown"


def _payload_json(payload: dict[str, Any]) -> str:
    return json.dumps(
        _sanitize_payload(payload),
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _sanitize_payload(value: Any) -> Any:
    if isinstance(value, dict):
        sanitized: dict[str, Any] = {}
        for key, child in value.items():
            if key.lower() in _REDACTED_PAYLOAD_KEYS:
                sanitized[key] = "[redacted]"
            else:
                sanitized[key] = _sanitize_payload(child)
        return sanitized
    if isinstance(value, list):
        return [_sanitize_payload(item) for item in value]
    if isinstance(value, bytes):
        return "[redacted-bytes]"
    return value
