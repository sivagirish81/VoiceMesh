from functools import lru_cache

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=False)

    environment: str = "development"
    log_level: str = "INFO"
    openai_api_key: str | None = None
    stt_provider: str = "openai"
    llm_provider: str = "openai"
    tts_provider: str = "openai"
    openai_stt_model: str = "gpt-realtime-whisper"
    openai_stt_language: str | None = "en"
    openai_stt_delay: str = "low"
    openai_llm_model: str = "gpt-4.1-mini"
    openai_tts_model: str = "gpt-4o-mini-tts"
    openai_tts_voice: str = "alloy"

    database_url: str = "postgresql://postgres:postgres@localhost:5432/voice_lab"
    database_pool_min_size: int = 2
    database_pool_max_size: int = 10
    database_command_timeout: float = 5.0
    kafka_bootstrap_servers: str = "localhost:9094"
    temporal_address: str = "localhost:7233"
    temporal_namespace: str = "default"
    temporal_task_queue: str = "voicemesh-calls"
    otel_exporter_otlp_endpoint: str = "http://localhost:4317"
    billing_platform_rate_per_minute_usd: float = Field(default=0.05, ge=0)
    billing_pricing_version: str = "openai-2026-06-15+voicemesh-lab-v1"
    billing_required_usage_types: str = (
        "stt_audio_seconds,llm_input_tokens,llm_output_tokens,tts_characters,tts_audio_seconds"
    )
    billing_usage_wait_seconds: int = Field(default=20, ge=1)
    billing_usage_settle_seconds: int = Field(default=3, ge=0)
    billing_missing_usage_policy: str = "FINALIZED_WITH_WARNINGS"
    kafka_consumer_batch_size: int = Field(default=100, ge=1)
    kafka_consumer_batch_timeout_ms: int = Field(default=500, ge=1)
    event_worker_metrics_port: int = Field(default=9101, ge=1)
    temporal_worker_metrics_port: int = Field(default=9102, ge=1)
    durable_action_default_timeout_seconds: int = Field(default=3600, ge=1)
    webhook_max_attempts: int = Field(default=5, ge=1)
    webhook_backoff_seconds: int = Field(default=2, ge=0)

    backpressure_high_watermark: int = Field(default=10, ge=2)
    backpressure_low_watermark: int = Field(default=3, ge=0)
    flow_queue_max_items: int = Field(default=128, ge=4)
    speech_chars_per_second: float = Field(default=14.0, gt=0)
    llm_to_tts_low_watermark_speak_ahead_ms: float = Field(default=300, ge=0)
    llm_to_tts_high_watermark_speak_ahead_ms: float = Field(default=1200, gt=0)
    llm_to_tts_hard_limit_speak_ahead_ms: float = Field(default=2500, gt=0)
    tts_to_transport_low_watermark_audio_ms: float = Field(default=300, ge=0)
    tts_to_transport_high_watermark_audio_ms: float = Field(default=1200, gt=0)
    tts_to_transport_hard_limit_audio_ms: float = Field(default=2500, gt=0)
    backpressure_hard_limit_policy: str = "cancel_response"
    tts_output_sample_rate: int = Field(default=24000, ge=8000)
    turn_timeout_seconds: float = Field(default=30, gt=0)
    vad_energy_threshold: float = Field(default=0.018, gt=0)
    vad_silence_ms: int = Field(default=700, ge=100)
    websocket_max_audio_bytes: int = 25 * 1024 * 1024

    failure_injection_enabled: bool = False
    inject_tts_delay_ms: int = 0
    inject_llm_delay_ms: int = 0
    inject_stt_delay_ms: int = 0
    inject_provider_failure: bool = False
    inject_postgres_failure: bool = False
    inject_stage_timeout: bool = False

    @model_validator(mode="after")
    def validate_watermarks(self) -> "Settings":
        if self.backpressure_low_watermark >= self.backpressure_high_watermark:
            raise ValueError("BACKPRESSURE_LOW_WATERMARK must be below the high watermark")
        if (
            self.llm_to_tts_low_watermark_speak_ahead_ms
            >= self.llm_to_tts_high_watermark_speak_ahead_ms
        ):
            raise ValueError("LLM_TO_TTS low watermark must be below high watermark")
        if (
            self.llm_to_tts_high_watermark_speak_ahead_ms
            > self.llm_to_tts_hard_limit_speak_ahead_ms
        ):
            raise ValueError("LLM_TO_TTS high watermark must be below hard limit")
        if (
            self.tts_to_transport_low_watermark_audio_ms
            >= self.tts_to_transport_high_watermark_audio_ms
        ):
            raise ValueError("TTS_TO_TRANSPORT low watermark must be below high watermark")
        if (
            self.tts_to_transport_high_watermark_audio_ms
            > self.tts_to_transport_hard_limit_audio_ms
        ):
            raise ValueError("TTS_TO_TRANSPORT high watermark must be below hard limit")
        if self.backpressure_hard_limit_policy not in {
            "cancel_response",
            "drop_oldest",
            "fail_turn",
        }:
            raise ValueError("BACKPRESSURE_HARD_LIMIT_POLICY is not supported")
        return self

    def validate_provider_credentials(self) -> None:
        selected = {self.stt_provider, self.llm_provider, self.tts_provider}
        if "openai" in selected and not self.openai_api_key:
            raise RuntimeError(
                "OPENAI_API_KEY is required when any selected provider is 'openai'. "
                "Set it in .env; VoiceMesh never silently falls back to a fake provider."
            )


@lru_cache
def get_settings() -> Settings:
    return Settings()
