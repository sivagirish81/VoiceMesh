from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class TranscriptionResult:
    transcript: str
    item_id: str
    audio_seconds: float


@dataclass(slots=True)
class TokenUsage:
    input_tokens: int = 0
    cached_input_tokens: int = 0
    output_tokens: int = 0


@dataclass(slots=True)
class SpeechUsage:
    input_text: str
    output_audio_bytes: int
    output_audio_seconds: float


class VADProvider(ABC):
    @abstractmethod
    async def detect_speech(self, audio_chunk: bytes) -> bool: ...


class StreamingSTTSession(ABC):
    @abstractmethod
    async def append_audio(self, audio_chunk: bytes, sample_rate: int) -> None: ...

    @abstractmethod
    async def commit(self) -> TranscriptionResult: ...

    @abstractmethod
    async def close(self) -> None: ...


class STTProvider(ABC):
    name: str
    model: str

    @abstractmethod
    async def open_stream(
        self, on_delta: Any
    ) -> StreamingSTTSession: ...


class LLMProvider(ABC):
    name: str
    model: str

    @abstractmethod
    def stream_generate(
        self, transcript: str, call_context: dict[str, Any]
    ) -> AsyncIterator[str]: ...

    @abstractmethod
    def consume_usage(self) -> TokenUsage: ...


class TTSProvider(ABC):
    name: str
    model: str

    @abstractmethod
    def synthesize(self, text: str) -> AsyncIterator[bytes]: ...

    @abstractmethod
    def consume_usage(self) -> SpeechUsage: ...


class Transport(ABC):
    @abstractmethod
    def receive_audio(self) -> AsyncIterator[bytes]: ...

    @abstractmethod
    async def send_audio(self, audio_chunk: bytes) -> None: ...
