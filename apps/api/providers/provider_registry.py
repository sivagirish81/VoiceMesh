from apps.api.config import Settings
from apps.api.failure_injection.injector import FailureInjector
from apps.api.providers.base import LLMProvider, STTProvider, TTSProvider
from apps.api.providers.openai_llm import OpenAILLMProvider
from apps.api.providers.openai_stt import OpenAISTTProvider
from apps.api.providers.openai_tts import OpenAITTSProvider


class ProviderRegistry:
    def __init__(self, settings: Settings, failure_injector: FailureInjector) -> None:
        self._settings = settings
        self._failure_injector = failure_injector

    def _api_key(self) -> str:
        self._settings.validate_provider_credentials()
        assert self._settings.openai_api_key is not None
        return self._settings.openai_api_key

    def stt(self) -> STTProvider:
        if self._settings.stt_provider == "openai":
            return OpenAISTTProvider(
                self._api_key(), self._settings.openai_stt_model, self._failure_injector
            )
        raise ValueError(f"Unknown STT provider: {self._settings.stt_provider}")

    def llm(self) -> LLMProvider:
        if self._settings.llm_provider == "openai":
            return OpenAILLMProvider(
                self._api_key(), self._settings.openai_llm_model, self._failure_injector
            )
        raise ValueError(f"Unknown LLM provider: {self._settings.llm_provider}")

    def tts(self) -> TTSProvider:
        if self._settings.tts_provider == "openai":
            return OpenAITTSProvider(
                self._api_key(),
                self._settings.openai_tts_model,
                self._settings.openai_tts_voice,
                self._failure_injector,
            )
        raise ValueError(f"Unknown TTS provider: {self._settings.tts_provider}")

