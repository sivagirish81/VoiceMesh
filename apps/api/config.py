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
    openai_stt_model: str = "gpt-4o-transcribe"
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

    backpressure_high_watermark: int = Field(default=10, ge=2)
    backpressure_low_watermark: int = Field(default=3, ge=0)
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

