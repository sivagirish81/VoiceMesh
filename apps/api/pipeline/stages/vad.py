import math
from array import array

from apps.api.providers.base import VADProvider


class EnergyVADProvider(VADProvider):
    """Energy VAD for real little-endian signed 16-bit mono microphone PCM."""

    def __init__(self, threshold: float) -> None:
        self.threshold = threshold

    async def detect_speech(self, audio_chunk: bytes) -> bool:
        if len(audio_chunk) < 2:
            return False
        samples = array("h")
        samples.frombytes(audio_chunk)
        if not samples:
            return False
        rms = math.sqrt(sum(sample * sample for sample in samples) / len(samples))
        normalized = rms / 32768.0
        return normalized >= self.threshold


class TurnDetector:
    def __init__(self, silence_ms: int) -> None:
        self.silence_ms = silence_ms
        self.speaking = False
        self.silence_accumulated_ms = 0.0

    def update(self, speech: bool, chunk_duration_ms: float) -> tuple[bool, bool]:
        started = False
        ended = False
        if speech:
            if not self.speaking:
                self.speaking = True
                started = True
            self.silence_accumulated_ms = 0
        elif self.speaking:
            self.silence_accumulated_ms += chunk_duration_ms
            if self.silence_accumulated_ms >= self.silence_ms:
                self.speaking = False
                self.silence_accumulated_ms = 0
                ended = True
        return started, ended

