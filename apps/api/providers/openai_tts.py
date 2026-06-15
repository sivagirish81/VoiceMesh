from collections.abc import AsyncIterator

from openai import AsyncOpenAI

from apps.api.failure_injection.injector import FailureInjector
from apps.api.providers.base import SpeechUsage, TTSProvider


class OpenAITTSProvider(TTSProvider):
    name = "openai"

    def __init__(
        self,
        api_key: str,
        model: str,
        voice: str,
        failure_injector: FailureInjector | None = None,
    ) -> None:
        self._client = AsyncOpenAI(api_key=api_key)
        self.model = model
        self._voice = voice
        self._failure_injector = failure_injector
        self._usage = SpeechUsage("", 0, 0.0)

    async def synthesize(self, text: str) -> AsyncIterator[bytes]:
        if self._failure_injector:
            await self._failure_injector.before_provider("tts")
        async with self._client.audio.speech.with_streaming_response.create(
            model=self.model,
            voice=self._voice,
            input=text,
            instructions="Speak naturally, warmly, and concisely.",
            response_format="pcm",
        ) as response:
            output_bytes = 0
            async for chunk in response.iter_bytes(chunk_size=4096):
                if self._failure_injector:
                    await self._failure_injector.delay("tts")
                output_bytes += len(chunk)
                yield chunk
        self._usage = SpeechUsage(
            input_text=text,
            output_audio_bytes=output_bytes,
            output_audio_seconds=output_bytes / (24_000 * 2),
        )

    def consume_usage(self) -> SpeechUsage:
        usage = self._usage
        self._usage = SpeechUsage("", 0, 0.0)
        return usage
