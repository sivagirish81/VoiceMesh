from prometheus_client import Counter, Gauge, Histogram

STAGE_LATENCY = Histogram(
    "voicemesh_stage_latency_ms",
    "Pipeline stage latency in milliseconds",
    ["stage", "provider"],
    buckets=(10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000),
)
QUEUE_DEPTH = Gauge("voicemesh_queue_depth", "Current pipeline queue depth", ["call_id", "stage"])
BACKPRESSURE_TOTAL = Counter(
    "voicemesh_backpressure_total", "Pipeline cork transitions", ["reason"]
)
BACKPRESSURE_SECONDS = Histogram(
    "voicemesh_backpressure_duration_seconds", "Time spent corked", ["reason"]
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

