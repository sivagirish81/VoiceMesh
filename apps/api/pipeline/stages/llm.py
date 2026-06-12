from collections.abc import AsyncIterator
from typing import Any

from apps.api.providers.base import LLMProvider


async def generate_tokens(
    provider: LLMProvider, transcript: str, context: dict[str, Any]
) -> AsyncIterator[str]:
    async for token in provider.stream_generate(transcript, context):
        yield token

