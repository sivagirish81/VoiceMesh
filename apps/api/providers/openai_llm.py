from collections.abc import AsyncIterator
from typing import Any

from openai import AsyncOpenAI

from apps.api.failure_injection.injector import FailureInjector
from apps.api.providers.base import LLMProvider, TokenUsage


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
                yield event.delta
            elif event.type == "response.completed" and event.response.usage:
                details = event.response.usage.input_tokens_details
                self._usage = TokenUsage(
                    input_tokens=event.response.usage.input_tokens,
                    cached_input_tokens=details.cached_tokens,
                    output_tokens=event.response.usage.output_tokens,
                )

    def consume_usage(self) -> TokenUsage:
        usage = self._usage
        self._usage = TokenUsage()
        return usage
