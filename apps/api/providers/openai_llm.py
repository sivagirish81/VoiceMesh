from collections.abc import AsyncIterator
from typing import Any

from openai import AsyncOpenAI

from apps.api.failure_injection.injector import FailureInjector
from apps.api.providers.base import LLMProvider


class OpenAILLMProvider(LLMProvider):
    name = "openai"

    def __init__(
        self, api_key: str, model: str, failure_injector: FailureInjector | None = None
    ) -> None:
        self._client = AsyncOpenAI(api_key=api_key)
        self._model = model
        self._failure_injector = failure_injector

    async def stream_generate(
        self, transcript: str, call_context: dict[str, Any]
    ) -> AsyncIterator[str]:
        if self._failure_injector:
            await self._failure_injector.before_provider("llm")
        stream = await self._client.responses.create(
            model=self._model,
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

