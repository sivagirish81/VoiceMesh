import asyncio
from dataclasses import asdict, dataclass

from apps.api.config import Settings


@dataclass
class FailureState:
    enabled: bool = False
    stt_delay_ms: int = 0
    llm_delay_ms: int = 0
    tts_delay_ms: int = 0
    provider_failure: bool = False
    postgres_failure: bool = False
    stage_timeout: bool = False


class FailureInjector:
    def __init__(self, settings: Settings) -> None:
        self._state = FailureState(
            enabled=settings.failure_injection_enabled,
            stt_delay_ms=settings.inject_stt_delay_ms,
            llm_delay_ms=settings.inject_llm_delay_ms,
            tts_delay_ms=settings.inject_tts_delay_ms,
            provider_failure=settings.inject_provider_failure,
            postgres_failure=settings.inject_postgres_failure,
            stage_timeout=settings.inject_stage_timeout,
        )
        self._lock = asyncio.Lock()

    async def update(self, values: dict[str, object]) -> dict[str, object]:
        async with self._lock:
            for key, value in values.items():
                if hasattr(self._state, key):
                    setattr(self._state, key, value)
            return asdict(self._state)

    async def reset(self) -> dict[str, object]:
        async with self._lock:
            self._state = FailureState()
            return asdict(self._state)

    def snapshot(self) -> dict[str, object]:
        return asdict(self._state)

    async def delay(self, stage: str) -> None:
        if not self._state.enabled:
            return
        delay_ms = int(getattr(self._state, f"{stage}_delay_ms", 0))
        if delay_ms:
            await asyncio.sleep(delay_ms / 1000)

    async def before_provider(self, stage: str) -> None:
        await self.delay(stage)
        if self._state.enabled and self._state.provider_failure:
            raise RuntimeError(f"Injected {stage} provider failure")
        if self._state.enabled and self._state.stage_timeout:
            await asyncio.sleep(3600)

    @property
    def postgres_failure(self) -> bool:
        return self._state.enabled and self._state.postgres_failure

