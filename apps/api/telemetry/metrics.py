from prometheus_client import Counter, Gauge, Histogram, start_http_server

_started_metrics_ports: set[int] = set()


def start_metrics_http_server(port: int) -> None:
    """Expose Prometheus metrics for non-FastAPI worker processes."""
    if port in _started_metrics_ports:
        return
    start_http_server(port)
    _started_metrics_ports.add(port)

STAGE_LATENCY = Histogram(
    "voicemesh_stage_latency_ms",
    "Pipeline stage latency in milliseconds",
    ["stage", "provider"],
    buckets=(10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000),
)
QUEUE_DEPTH = Gauge(
    "voicemesh_queue_depth",
    "Current weighted pipeline queue depth",
    ["stage", "depth_unit"],
)
QUEUE_ITEMS = Gauge(
    "voicemesh_queue_items",
    "Current pipeline queue item count for debug visibility",
    ["stage"],
)
BACKPRESSURE_TOTAL = Counter(
    "voicemesh_backpressure_total",
    "Pipeline cork/uncork transitions",
    ["stage", "transition", "reason_code", "depth_unit"],
)
BACKPRESSURE_SECONDS = Histogram(
    "voicemesh_backpressure_duration_seconds",
    "Time spent corked",
    ["stage"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60),
)
DUPLICATE_EVENTS = Counter(
    "voicemesh_duplicate_events_total", "Duplicate events ignored", ["event_type"]
)
PROVIDER_FAILURES = Counter(
    "voicemesh_provider_failures_total", "Provider failures", ["provider", "stage"]
)
PROVIDER_FALLBACKS = Counter(
    "voicemesh_provider_fallbacks_total", "Provider fallback selections", ["stage"]
)
DB_WRITE_FAILURES = Counter("voicemesh_db_write_failures_total", "Postgres write failures")
ACTIVE_CALLS = Gauge("voicemesh_active_calls", "Active WebSocket calls")
HARD_LIMIT_TOTAL = Counter(
    "voicemesh_backpressure_hard_limit_total",
    "Hard-limit flow-control actions",
    ["stage", "depth_unit", "policy"],
)
STALE_CHUNKS_DROPPED_TOTAL = Counter(
    "voicemesh_stale_chunks_dropped_total",
    "Stale chunks dropped before provider or transport use",
    ["stage", "chunk_type", "reason_code"],
)
STALE_AUDIO_DROPPED_MS_TOTAL = Counter(
    "voicemesh_stale_audio_dropped_ms_total",
    "Playable stale audio duration dropped before transport",
    ["stage", "reason_code"],
)

CALL_EVENTS_TOTAL = Counter(
    "voicemesh_call_events_total",
    "Coarse call lifecycle events projected from Kafka",
    ["event_type"],
)
LLM_FIRST_TOKEN_LATENCY = Histogram(
    "voicemesh_llm_first_token_latency_ms",
    "LLM time to first token in milliseconds",
    ["provider", "model"],
    buckets=(10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000),
)
TTS_FIRST_AUDIO_LATENCY = Histogram(
    "voicemesh_tts_first_audio_latency_ms",
    "TTS time to first audio chunk in milliseconds",
    ["provider", "model"],
    buckets=(10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000),
)

KAFKA_EVENTS_CONSUMED_TOTAL = Counter(
    "voicemesh_kafka_events_consumed_total",
    "Kafka events decoded by the event worker",
    ["topic", "event_type", "consumer_group"],
)
KAFKA_CONSUMER_LAG = Gauge(
    "voicemesh_kafka_consumer_lag",
    "Kafka consumer lag observed by the event worker",
    ["topic", "partition", "consumer_group"],
)
KAFKA_BATCH_SIZE = Histogram(
    "voicemesh_kafka_consumer_batch_size",
    "Kafka event worker consumed batch size",
    ["consumer_group"],
    buckets=(1, 2, 5, 10, 25, 50, 100, 250, 500),
)
KAFKA_BATCH_DURATION = Histogram(
    "voicemesh_kafka_consumer_batch_duration_seconds",
    "Kafka event worker batch handling duration",
    ["consumer_group"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)

POSTGRES_PROJECTION_DURATION = Histogram(
    "voicemesh_postgres_projection_duration_seconds",
    "Postgres event projection transaction duration",
    ["operation"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)
POSTGRES_PROJECTED_EVENTS_TOTAL = Counter(
    "voicemesh_postgres_projected_events_total",
    "Events successfully projected into Postgres",
    ["event_type"],
)
POSTGRES_PROJECTION_ERRORS_TOTAL = Counter(
    "voicemesh_postgres_projection_errors_total",
    "Postgres projection failures",
    ["operation"],
)

TEMPORAL_WORKFLOWS_TOTAL = Counter(
    "voicemesh_temporal_workflows_total",
    "Temporal workflow lifecycle events observed by VoiceMesh",
    ["workflow_type", "status"],
)
TEMPORAL_ACTIVITIES_TOTAL = Counter(
    "voicemesh_temporal_activities_total",
    "Temporal activity executions observed by VoiceMesh",
    ["activity_name", "status"],
)
TEMPORAL_ACTIVITY_DURATION = Histogram(
    "voicemesh_temporal_activity_duration_seconds",
    "Temporal activity execution duration",
    ["activity_name"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
)
BILLING_WORKFLOWS_WAITING = Counter(
    "voicemesh_billing_workflows_waiting",
    "Billing readiness checks observed in waiting states",
    ["state"],
)
BILLING_FINALIZATION_DURATION = Histogram(
    "voicemesh_billing_finalization_duration_seconds",
    "Billing finalization activity duration",
    ["status"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
)
BILLING_ADJUSTMENTS_TOTAL = Counter(
    "voicemesh_billing_adjustments_total",
    "Billing adjustment workflows created",
    ["reason"],
)
WEBHOOK_DELIVERIES_TOTAL = Counter(
    "voicemesh_webhook_deliveries_total",
    "Webhook delivery state transitions",
    ["status"],
)
WEBHOOK_DELIVERY_ATTEMPTS_TOTAL = Counter(
    "voicemesh_webhook_delivery_attempts_total",
    "Webhook delivery attempts",
    ["status"],
)
WEBHOOK_DELIVERY_DURATION = Histogram(
    "voicemesh_webhook_delivery_duration_seconds",
    "Webhook delivery attempt duration",
    ["status"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
)
