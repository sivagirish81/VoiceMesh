from apps.api.config import Settings
from apps.api.failure_injection.injector import FailureInjector
from apps.api.providers.base import LLMProvider, STTProvider, TTSProvider
from apps.api.providers.openai_llm import OpenAILLMProvider
from apps.api.providers.openai_stt import OpenAISTTProvider
from apps.api.providers.openai_tts import OpenAITTSProvider


class ProviderRegistry:
    def __init__(
        self,
        settings: Settings,
        failure_injector: FailureInjector,
        agent_config: dict[str, object] | None = None,
    ) -> None:
        self._settings = settings
        self._failure_injector = failure_injector
        self._agent_config = agent_config or {}

    def _config_value(self, key: str, fallback: str) -> str:
        value = self._agent_config.get(key)
        return str(value) if value else fallback

    def _api_key(self) -> str:
        self._settings.validate_provider_credentials()
        assert self._settings.openai_api_key is not None
        return self._settings.openai_api_key

    def stt(self) -> STTProvider:
        provider = self._config_value("stt_provider", self._settings.stt_provider)
        if provider == "openai":
            return OpenAISTTProvider(
                self._api_key(),
                self._config_value("stt_model", self._settings.openai_stt_model),
                self._failure_injector,
                language=self._settings.openai_stt_language,
                delay=self._settings.openai_stt_delay,
            )
        raise ValueError(f"Unknown STT provider: {provider}")

    def llm(self) -> LLMProvider:
        provider = self._config_value("llm_provider", self._settings.llm_provider)
        if provider == "openai":
            return OpenAILLMProvider(
                self._api_key(),
                self._config_value("llm_model", self._settings.openai_llm_model),
                self._failure_injector,
            )
        raise ValueError(f"Unknown LLM provider: {provider}")

    def tts(self) -> TTSProvider:
        provider = self._config_value("tts_provider", self._settings.tts_provider)
        if provider == "openai":
            return OpenAITTSProvider(
                self._api_key(),
                self._config_value("tts_model", self._settings.openai_tts_model),
                self._config_value("tts_voice", self._settings.openai_tts_voice),
                self._failure_injector,
            )
        raise ValueError(f"Unknown TTS provider: {provider}")
