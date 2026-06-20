from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class AnalyticsEventRow:
    event_id: str
    event_type: str
    event_version: int
    event_time: datetime
    tenant_id: str
    assistant_id: str
    call_id: str
    turn_id: str
    response_id: str
    sequence: int
    stage: str
    provider: str
    model: str
    status: str
    reason_code: str
    latency_ms: float | None
    duration_ms: float | None
    quantity: float | None
    unit: str
    trace_id: str
    idempotency_key: str
    payload_json: str


CLICKHOUSE_COLUMNS = [
    "event_id",
    "event_type",
    "event_version",
    "event_time",
    "tenant_id",
    "assistant_id",
    "call_id",
    "turn_id",
    "response_id",
    "sequence",
    "stage",
    "provider",
    "model",
    "status",
    "reason_code",
    "latency_ms",
    "duration_ms",
    "quantity",
    "unit",
    "trace_id",
    "idempotency_key",
    "payload_json",
]


def row_values(row: AnalyticsEventRow) -> list[object]:
    return [getattr(row, column) for column in CLICKHOUSE_COLUMNS]
