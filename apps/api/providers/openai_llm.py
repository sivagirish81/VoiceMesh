import time
from collections.abc import AsyncIterator
from typing import Any

from openai import AsyncOpenAI
from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode

from apps.api.failure_injection.injector import FailureInjector
from apps.api.providers.base import LLMProvider, TokenUsage
from apps.api.telemetry.tracing import set_span_attributes

tracer = trace.get_tracer(__name__)


class OpenAILLMProvider(LLMProvider):
    name = "openai"

    def __init__(
        self, api_key: str, model: str, failure_injector: FailureInjector | None = None
    ) -> None:
        self._client = AsyncOpenAI(api_key=api_key)
        self.model = model
        self._failure_injector = failure_injector
        self._usage = TokenUsage()

    async def stream_generate(
        self, transcript: str, call_context: dict[str, Any]
    ) -> AsyncIterator[str]:
        span = tracer.start_span("provider.openai.llm.responses_stream", kind=SpanKind.CLIENT)
        started = time.perf_counter()
        first_token_ms: float | None = None
        output_chars = 0
        output_chunks = 0
        try:
            set_span_attributes(
                span,
                provider="openai",
                provider_stage="llm",
                model=self.model,
                endpoint="https://api.openai.com/v1/responses",
                stream=True,
                call_id=call_context.get("call_id"),
                transcript_chars=len(transcript),
            )
            if self._failure_injector:
                await self._failure_injector.before_provider("llm")
            stream = await self._client.responses.create(
                model=self.model,
                input=[
                    {
                        "role": "system",
                        "content": (
                            "You are the voice assistant inside VoiceMesh, a reliability lab. "
                            "Answer clearly and conversationally in two or three short sentences."
                        ),
                    },
                    {"role": "user", "content": transcript},
                ],
                stream=True,
            )
            async for event in stream:
                if event.type == "response.output_text.delta":
                    if self._failure_injector:
                        await self._failure_injector.delay("llm")
                    if first_token_ms is None:
                        first_token_ms = (time.perf_counter() - started) * 1000
                        set_span_attributes(span, first_token_ms=first_token_ms)
                    output_chars += len(event.delta)
                    output_chunks += 1
                    yield event.delta
                elif event.type == "response.completed" and event.response.usage:
                    details = event.response.usage.input_tokens_details
                    self._usage = TokenUsage(
                        input_tokens=event.response.usage.input_tokens,
                        cached_input_tokens=details.cached_tokens,
                        output_tokens=event.response.usage.output_tokens,
                    )
                    set_span_attributes(
                        span,
                        input_tokens=self._usage.input_tokens,
                        cached_input_tokens=self._usage.cached_input_tokens,
                        output_tokens=self._usage.output_tokens,
                    )
            set_span_attributes(
                span,
                output_chars=output_chars,
                output_chunks=output_chunks,
            )
        except Exception as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, str(exc)))
            raise
        finally:
            set_span_attributes(
                span,
                latency_ms=(time.perf_counter() - started) * 1000,
                first_token_ms=first_token_ms,
                output_chars=output_chars,
                output_chunks=output_chunks,
            )
            span.end()

    def consume_usage(self) -> TokenUsage:
        usage = self._usage
        self._usage = TokenUsage()
        return usage
