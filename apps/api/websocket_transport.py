import base64
import json
from collections.abc import AsyncIterator
from typing import Any

from fastapi import WebSocket
from opentelemetry import trace

from apps.api.pipeline.events import AudioFrame
from apps.api.providers.base import Transport

tracer = trace.get_tracer(__name__)


class BrowserWebSocketTransport(Transport):
    def __init__(self, websocket: WebSocket) -> None:
        self.websocket = websocket
        self.sample_rate = 16000
        self.channels = 1
        self._closed = False

    async def accept(self) -> None:
        await self.websocket.accept()

    async def receive_messages(self) -> AsyncIterator[AudioFrame | dict[str, Any]]:
        while not self._closed:
            with tracer.start_as_current_span("websocket.receive"):
                message = await self.websocket.receive()
            if message["type"] == "websocket.disconnect":
                self._closed = True
                return
            if message.get("bytes") is not None:
                yield AudioFrame(
                    data=message["bytes"],
                    sample_rate=self.sample_rate,
                    channels=self.channels,
                )
                continue
            if message.get("text"):
                payload = json.loads(message["text"])
                if payload.get("type") == "audio.config":
                    self.sample_rate = int(payload.get("sample_rate", 16000))
                    self.channels = int(payload.get("channels", 1))
                yield payload

    async def receive_audio(self) -> AsyncIterator[bytes]:
        async for message in self.receive_messages():
            if isinstance(message, AudioFrame):
                yield message.data

    async def send_json(self, event_type: str, **payload: Any) -> None:
        with tracer.start_as_current_span("websocket.send") as span:
            span.set_attribute("message.type", event_type)
            await self.websocket.send_json({"type": event_type, **payload})

    async def send_audio(self, audio_chunk: bytes) -> None:
        await self.send_json(
            "audio.chunk",
            audio=base64.b64encode(audio_chunk).decode(),
            encoding="pcm_s16le",
            sample_rate=24000,
            channels=1,
        )

    async def close(self, code: int = 1000) -> None:
        if not self._closed:
            self._closed = True
            await self.websocket.close(code=code)
