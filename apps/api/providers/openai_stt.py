import io

from openai import AsyncOpenAI

from apps.api.failure_injection.injector import FailureInjector
from apps.api.providers.base import STTProvider


class OpenAISTTProvider(STTProvider):
    name = "openai"

    def __init__(
        self, api_key: str, model: str, failure_injector: FailureInjector | None = None
    ) -> None:
        self._client = AsyncOpenAI(api_key=api_key)
        self._model = model
        self._failure_injector = failure_injector

    async def transcribe(self, audio_bytes: bytes) -> str:
        if self._failure_injector:
            await self._failure_injector.before_provider("stt")
        audio_file = io.BytesIO(audio_bytes)
        audio_file.name = "turn.wav"
        result = await self._client.audio.transcriptions.create(
            model=self._model,
            file=audio_file,
            response_format="text",
        )
        return result if isinstance(result, str) else result.text
