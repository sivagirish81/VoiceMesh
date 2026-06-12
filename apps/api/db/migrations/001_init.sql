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
    final_summary TEXT,
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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

INSERT INTO provider_configs (provider_type, provider_name, config)
VALUES
    ('stt', 'openai', '{"model": "gpt-4o-transcribe"}'),
    ('llm', 'openai', '{"model": "gpt-4.1-mini"}'),
    ('tts', 'openai', '{"model": "gpt-4o-mini-tts", "voice": "alloy"}')
ON CONFLICT (provider_type, provider_name) DO NOTHING;

