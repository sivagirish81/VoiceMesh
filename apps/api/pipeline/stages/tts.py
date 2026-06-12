from collections.abc import AsyncIterator

from apps.api.providers.base import TTSProvider


async def synthesize_audio(provider: TTSProvider, text: str) -> AsyncIterator[bytes]:
    async for chunk in provider.synthesize(text):
        yield chunk

