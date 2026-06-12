import pytest

from apps.api.config import Settings
from apps.api.failure_injection.injector import FailureInjector
from apps.api.providers.openai_llm import OpenAILLMProvider
from apps.api.providers.openai_stt import OpenAISTTProvider
from apps.api.providers.openai_tts import OpenAITTSProvider
from apps.api.providers.provider_registry import ProviderRegistry


def test_registry_loads_openai_providers() -> None:
    settings = Settings(openai_api_key="test-key")
    registry = ProviderRegistry(settings, FailureInjector(settings))
    assert isinstance(registry.stt(), OpenAISTTProvider)
    assert isinstance(registry.llm(), OpenAILLMProvider)
    assert isinstance(registry.tts(), OpenAITTSProvider)


def test_missing_openai_key_fails_fast() -> None:
    settings = Settings(openai_api_key=None)
    registry = ProviderRegistry(settings, FailureInjector(settings))
    with pytest.raises(RuntimeError, match="OPENAI_API_KEY"):
        registry.stt()


def test_unknown_provider_does_not_fall_back() -> None:
    settings = Settings(openai_api_key="test-key", stt_provider="local-whisper")
    registry = ProviderRegistry(settings, FailureInjector(settings))
    with pytest.raises(ValueError, match="Unknown STT provider"):
        registry.stt()

