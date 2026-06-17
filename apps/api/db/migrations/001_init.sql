CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS calls (
    id BIGSERIAL PRIMARY KEY,
    call_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    current_stage TEXT NOT NULL DEFAULT 'transport',
    corked BOOLEAN NOT NULL DEFAULT FALSE,
    cork_reason TEXT,
    selected_stt_provider TEXT NOT NULL,
    selected_llm_provider TEXT NOT NULL,
    selected_tts_provider TEXT NOT NULL,
    selected_stt_model TEXT,
    selected_llm_model TEXT,
    selected_tts_model TEXT,
    final_summary TEXT,
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE calls ADD COLUMN IF NOT EXISTS selected_stt_model TEXT;
ALTER TABLE calls ADD COLUMN IF NOT EXISTS selected_llm_model TEXT;
ALTER TABLE calls ADD COLUMN IF NOT EXISTS selected_tts_model TEXT;

CREATE TABLE IF NOT EXISTS call_events (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    call_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    stage TEXT NOT NULL,
    sequence_number BIGINT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    trace_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_call_events_call_id_created ON call_events(call_id, created_at);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    id BIGSERIAL PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    call_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS outbox_events (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    topic TEXT NOT NULL,
    key TEXT NOT NULL,
    payload JSONB NOT NULL,
    published BOOLEAN NOT NULL DEFAULT FALSE,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox_events(published, created_at);

CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id BIGSERIAL PRIMARY KEY,
    call_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    latency_ms DOUBLE PRECISION NOT NULL,
    queue_depth INTEGER NOT NULL,
    corked BOOLEAN NOT NULL,
    provider TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_call_stage ON pipeline_metrics(call_id, stage, created_at);

CREATE TABLE IF NOT EXISTS provider_configs (
    id BIGSERIAL PRIMARY KEY,
    provider_type TEXT NOT NULL,
    provider_name TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(provider_type, provider_name)
);

CREATE TABLE IF NOT EXISTS pricing_catalog (
    id BIGSERIAL PRIMARY KEY,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    usage_type TEXT NOT NULL,
    unit TEXT NOT NULL,
    unit_price_usd NUMERIC(20, 12) NOT NULL,
    pricing_version TEXT NOT NULL,
    effective_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(provider, model, usage_type, pricing_version)
);

CREATE TABLE IF NOT EXISTS usage_records (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL,
    call_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    usage_type TEXT NOT NULL,
    quantity NUMERIC(24, 8) NOT NULL,
    unit TEXT NOT NULL,
    unit_price_usd NUMERIC(20, 12) NOT NULL,
    cost_usd NUMERIC(20, 10) NOT NULL,
    estimated BOOLEAN NOT NULL DEFAULT FALSE,
    pricing_version TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(event_id, usage_type)
);
CREATE INDEX IF NOT EXISTS idx_usage_records_call_created
    ON usage_records(call_id, created_at);

CREATE TABLE IF NOT EXISTS call_billing (
    call_id TEXT PRIMARY KEY,
    call_duration_seconds NUMERIC(16, 3) NOT NULL DEFAULT 0,
    provider_cost_usd NUMERIC(20, 10) NOT NULL DEFAULT 0,
    platform_fee_usd NUMERIC(20, 10) NOT NULL DEFAULT 0,
    total_cost_usd NUMERIC(20, 10) NOT NULL DEFAULT 0,
    currency TEXT NOT NULL DEFAULT 'USD',
    status TEXT NOT NULL DEFAULT 'OPEN',
    pricing_version TEXT NOT NULL,
    finalized_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tool_invocations (
    id BIGSERIAL PRIMARY KEY,
    tool_invocation_id TEXT NOT NULL UNIQUE,
    tenant_id TEXT NOT NULL,
    assistant_id TEXT NOT NULL,
    call_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    execution_mode TEXT NOT NULL,
    workflow_id TEXT UNIQUE,
    external_request_id TEXT,
    status TEXT NOT NULL,
    arguments_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_error TEXT,
    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    cancel_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_tool_invocations_call
    ON tool_invocations(call_id, created_at);

CREATE TABLE IF NOT EXISTS tool_invocation_attempts (
    attempt_id BIGSERIAL PRIMARY KEY,
    tool_invocation_id TEXT NOT NULL,
    activity_name TEXT NOT NULL,
    attempt_number INTEGER NOT NULL DEFAULT 1,
    request_url TEXT,
    status_code INTEGER,
    success BOOLEAN NOT NULL DEFAULT FALSE,
    error TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_tool_invocation_attempts_tool
    ON tool_invocation_attempts(tool_invocation_id, started_at);

CREATE TABLE IF NOT EXISTS call_usage_events (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    tenant_id TEXT NOT NULL,
    assistant_id TEXT NOT NULL,
    call_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    usage_type TEXT NOT NULL,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    quantity NUMERIC(24, 8) NOT NULL,
    unit TEXT NOT NULL,
    cost_basis_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_call_usage_events_call
    ON call_usage_events(call_id, created_at);

CREATE TABLE IF NOT EXISTS call_usage_rollups (
    call_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    assistant_id TEXT NOT NULL,
    stt_audio_seconds NUMERIC(24, 8) NOT NULL DEFAULT 0,
    llm_input_tokens NUMERIC(24, 8) NOT NULL DEFAULT 0,
    llm_output_tokens NUMERIC(24, 8) NOT NULL DEFAULT 0,
    tts_characters NUMERIC(24, 8) NOT NULL DEFAULT 0,
    tts_audio_seconds NUMERIC(24, 8) NOT NULL DEFAULT 0,
    telephony_seconds NUMERIC(24, 8) NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'OPEN',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS final_call_billing_records (
    call_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    assistant_id TEXT NOT NULL,
    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    billable_seconds INTEGER NOT NULL DEFAULT 0,
    platform_cost_cents INTEGER NOT NULL DEFAULT 0,
    stt_cost_cents INTEGER NOT NULL DEFAULT 0,
    llm_cost_cents INTEGER NOT NULL DEFAULT 0,
    tts_cost_cents INTEGER NOT NULL DEFAULT 0,
    telephony_cost_cents INTEGER NOT NULL DEFAULT 0,
    total_cost_cents INTEGER NOT NULL DEFAULT 0,
    currency TEXT NOT NULL DEFAULT 'USD',
    pricing_version TEXT NOT NULL,
    status TEXT NOT NULL,
    warnings JSONB NOT NULL DEFAULT '[]'::jsonb,
    finalized_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS webhook_deliveries (
    webhook_delivery_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    assistant_id TEXT NOT NULL,
    call_id TEXT NOT NULL,
    workflow_id TEXT UNIQUE,
    target_url TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_status_code INTEGER,
    last_error TEXT,
    idempotency_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS webhook_delivery_attempts (
    attempt_id BIGSERIAL PRIMARY KEY,
    webhook_delivery_id TEXT NOT NULL,
    attempt_number INTEGER NOT NULL,
    status_code INTEGER,
    success BOOLEAN NOT NULL DEFAULT FALSE,
    error TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_webhook_delivery_attempts_delivery
    ON webhook_delivery_attempts(webhook_delivery_id, attempt_number);

INSERT INTO provider_configs (provider_type, provider_name, config)
VALUES
    ('stt', 'openai', '{"model": "gpt-realtime-whisper"}'),
    ('llm', 'openai', '{"model": "gpt-4.1-mini"}'),
    ('tts', 'openai', '{"model": "gpt-4o-mini-tts", "voice": "alloy"}')
ON CONFLICT (provider_type, provider_name) DO NOTHING;

INSERT INTO pricing_catalog (
    provider, model, usage_type, unit, unit_price_usd, pricing_version
)
VALUES
    ('openai', 'gpt-realtime-whisper', 'audio_minute', 'minute', 0.017, 'openai-2026-06-15'),
    ('openai', 'gpt-4.1-mini', 'input_token', 'token', 0.0000004, 'openai-2026-06-15'),
    ('openai', 'gpt-4.1-mini', 'cached_input_token', 'token', 0.0000001, 'openai-2026-06-15'),
    ('openai', 'gpt-4.1-mini', 'output_token', 'token', 0.0000016, 'openai-2026-06-15'),
    ('openai', 'gpt-4o-mini-tts', 'input_text_token', 'token', 0.0000006, 'openai-2026-06-15'),
    ('openai', 'gpt-4o-mini-tts', 'output_audio_token', 'token', 0.000012, 'openai-2026-06-15')
ON CONFLICT (provider, model, usage_type, pricing_version) DO UPDATE SET
    unit = EXCLUDED.unit,
    unit_price_usd = EXCLUDED.unit_price_usd,
    effective_at = NOW();
