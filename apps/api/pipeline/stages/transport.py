from apps.api.websocket_transport import BrowserWebSocketTransport


async def send_audio(transport: BrowserWebSocketTransport, audio_chunk: bytes) -> None:
    await transport.send_audio(audio_chunk)

