import asyncio
import base64
import json
from collections.abc import Awaitable, Callable
from contextlib import suppress
from typing import Any

from websockets.asyncio.client import ClientConnection, connect

from apps.api.failure_injection.injector import FailureInjector
from apps.api.pipeline.audio import resample_pcm16_mono
from apps.api.providers.base import (
    StreamingSTTSession,
    STTProvider,
    TranscriptionResult,
)

TranscriptDeltaCallback = Callable[[str], Awaitable[None]]


class OpenAIStreamingSTTSession(StreamingSTTSession):
    target_sample_rate = 24_000

    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        language: str | None,
        delay: str,
        on_delta: TranscriptDeltaCallback,
        failure_injector: FailureInjector | None,
    ) -> None:
        self._api_key = api_key
        self._model = model
        self._language = language
        self._delay = delay
        self._on_delta = on_delta
        self._failure_injector = failure_injector
        self._websocket: ClientConnection | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._ready: asyncio.Future[None] | None = None
        self._pending_result: asyncio.Future[TranscriptionResult] | None = None
        self._buffered_samples = 0
        self._commit_audio_seconds = 0.0
        self._commit_lock = asyncio.Lock()

    async def start(self) -> None:
        self._ready = asyncio.get_running_loop().create_future()
        self._websocket = await connect(
            "wss://api.openai.com/v1/realtime?intent=transcription",
            additional_headers={"Authorization": f"Bearer {self._api_key}"},
            max_size=8 * 1024 * 1024,
            ping_interval=20,
            ping_timeout=20,
        )
        self._reader_task = asyncio.create_task(self._read_events())
        transcription: dict[str, Any] = {
            "model": self._model,
            "delay": self._delay,
        }
        if self._language:
            transcription["language"] = self._language
        await self._send(
            {
                "type": "session.update",
                "session": {
                    "type": "transcription",
                    "audio": {
                        "input": {
                            "format": {"type": "audio/pcm", "rate": self.target_sample_rate},
                            "transcription": transcription,
                            "turn_detection": None,
                        }
                    },
                },
            }
        )
        async with asyncio.timeout(10):
            await self._ready

    async def append_audio(self, audio_chunk: bytes, sample_rate: int) -> None:
        normalized = resample_pcm16_mono(
            audio_chunk, source_rate=sample_rate, target_rate=self.target_sample_rate
        )
        if not normalized:
            return
        self._buffered_samples += len(normalized) // 2
        await self._send(
            {
                "type": "input_audio_buffer.append",
                "audio": base64.b64encode(normalized).decode("ascii"),
            }
        )

    async def commit(self) -> TranscriptionResult:
        async with self._commit_lock:
            if self._buffered_samples == 0:
                return TranscriptionResult("", "", 0.0)
            if self._failure_injector:
                await self._failure_injector.before_provider("stt")
            self._pending_result = asyncio.get_running_loop().create_future()
            self._commit_audio_seconds = self._buffered_samples / self.target_sample_rate
            self._buffered_samples = 0
            await self._send({"type": "input_audio_buffer.commit"})
            return await self._pending_result

    async def close(self) -> None:
        if self._websocket:
            await self._websocket.close()
            self._websocket = None
        if self._reader_task:
            if not self._reader_task.done():
                self._reader_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._reader_task
            self._reader_task = None

    async def _send(self, event: dict[str, Any]) -> None:
        if not self._websocket:
            raise RuntimeError("OpenAI streaming STT session is not connected")
        await self._websocket.send(json.dumps(event))

    async def _read_events(self) -> None:
        assert self._websocket is not None
        try:
            async for raw_event in self._websocket:
                event = json.loads(raw_event)
                event_type = event.get("type")
                if event_type in {"session.updated", "transcription_session.updated"}:
                    if self._ready and not self._ready.done():
                        self._ready.set_result(None)
                elif event_type == "conversation.item.input_audio_transcription.delta":
                    delta = str(event.get("delta") or "")
                    if delta:
                        await self._on_delta(delta)
                elif event_type == "conversation.item.input_audio_transcription.completed":
                    if self._pending_result and not self._pending_result.done():
                        self._pending_result.set_result(
                            TranscriptionResult(
                                transcript=str(event.get("transcript") or ""),
                                item_id=str(event.get("item_id") or ""),
                                audio_seconds=self._commit_audio_seconds,
                            )
                        )
                elif event_type == "error":
                    error = event.get("error") or {}
                    message = str(error.get("message") or "OpenAI streaming STT error")
                    self._fail_pending(RuntimeError(message))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._fail_pending(exc)

    def _fail_pending(self, exc: Exception) -> None:
        if self._ready and not self._ready.done():
            self._ready.set_exception(exc)
        if self._pending_result and not self._pending_result.done():
            self._pending_result.set_exception(exc)


class OpenAISTTProvider(STTProvider):
    name = "openai"

    def __init__(
        self,
        api_key: str,
        model: str,
        failure_injector: FailureInjector | None = None,
        *,
        language: str | None = "en",
        delay: str = "low",
    ) -> None:
        self._api_key = api_key
        self.model = model
        self._language = language
        self._delay = delay
        self._failure_injector = failure_injector

    async def open_stream(
        self, on_delta: TranscriptDeltaCallback
    ) -> StreamingSTTSession:
        session = OpenAIStreamingSTTSession(
            api_key=self._api_key,
            model=self.model,
            language=self._language,
            delay=self._delay,
            on_delta=on_delta,
            failure_injector=self._failure_injector,
        )
        await session.start()
        return session
