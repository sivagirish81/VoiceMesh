import asyncio
import io
import wave

from apps.api.providers.base import STTProvider


def pcm_to_wav(pcm_bytes: bytes, sample_rate: int) -> bytes:
    output = io.BytesIO()
    with wave.open(output, "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(sample_rate)
        wav.writeframes(pcm_bytes)
    return output.getvalue()


async def transcribe_turn(
    provider: STTProvider, pcm_bytes: bytes, sample_rate: int, timeout: float
) -> str:
    return await asyncio.wait_for(
        provider.transcribe(pcm_to_wav(pcm_bytes, sample_rate)),
        timeout=timeout,
    )

