import time
from collections.abc import AsyncIterator

from openai import AsyncOpenAI
from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode

from apps.api.failure_injection.injector import FailureInjector
from apps.api.providers.base import SpeechUsage, TTSProvider
from apps.api.telemetry.tracing import set_span_attributes

tracer = trace.get_tracer(__name__)


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

    @property
    def voice(self) -> str:
        return self._voice

    async def synthesize(self, text: str) -> AsyncIterator[bytes]:
        span = tracer.start_span("provider.openai.tts.speech_stream", kind=SpanKind.CLIENT)
        started = time.perf_counter()
        first_audio_ms: float | None = None
        output_bytes = 0
        output_chunks = 0
        try:
            set_span_attributes(
                span,
                provider="openai",
                provider_stage="tts",
                model=self.model,
                endpoint="https://api.openai.com/v1/audio/speech",
                voice=self._voice,
                response_format="pcm",
                input_text_chars=len(text),
            )
            if self._failure_injector:
                await self._failure_injector.before_provider("tts")
            async with self._client.audio.speech.with_streaming_response.create(
                model=self.model,
                voice=self._voice,
                input=text,
                instructions="Speak naturally, warmly, and concisely.",
                response_format="pcm",
            ) as response:
                async for chunk in response.iter_bytes(chunk_size=4096):
                    if self._failure_injector:
                        await self._failure_injector.delay("tts")
                    if first_audio_ms is None:
                        first_audio_ms = (time.perf_counter() - started) * 1000
                        set_span_attributes(span, first_audio_ms=first_audio_ms)
                    output_bytes += len(chunk)
                    output_chunks += 1
                    yield chunk
            self._usage = SpeechUsage(
                input_text=text,
                output_audio_bytes=output_bytes,
                output_audio_seconds=output_bytes / (24_000 * 2),
            )
        except Exception as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, str(exc)))
            raise
        finally:
            set_span_attributes(
                span,
                latency_ms=(time.perf_counter() - started) * 1000,
                first_audio_ms=first_audio_ms,
                output_audio_bytes=output_bytes,
                output_audio_chunks=output_chunks,
                output_audio_seconds=output_bytes / (24_000 * 2),
            )
            span.end()

    def consume_usage(self) -> SpeechUsage:
        usage = self._usage
        self._usage = SpeechUsage("", 0, 0.0)
        return usage

    async def cancel(self, response_id: str) -> None:
        with tracer.start_as_current_span("provider.openai.tts.cancel") as span:
            set_span_attributes(
                span,
                provider="openai",
                provider_stage="tts",
                model=self.model,
                response_id=response_id,
                cancellation_mode="local_fence",
            )
