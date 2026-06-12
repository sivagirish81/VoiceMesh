from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import Any


class VADProvider(ABC):
    @abstractmethod
    async def detect_speech(self, audio_chunk: bytes) -> bool: ...


class STTProvider(ABC):
    name: str

    @abstractmethod
    async def transcribe(self, audio_bytes: bytes) -> str: ...


class LLMProvider(ABC):
    name: str

    @abstractmethod
    def stream_generate(
        self, transcript: str, call_context: dict[str, Any]
    ) -> AsyncIterator[str]: ...


class TTSProvider(ABC):
    name: str

    @abstractmethod
    def synthesize(self, text: str) -> AsyncIterator[bytes]: ...


class Transport(ABC):
    @abstractmethod
    def receive_audio(self) -> AsyncIterator[bytes]: ...

    @abstractmethod
    async def send_audio(self, audio_chunk: bytes) -> None: ...

