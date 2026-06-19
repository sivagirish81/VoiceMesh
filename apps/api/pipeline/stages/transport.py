from apps.api.websocket_transport import BrowserWebSocketTransport


async def send_audio(
    transport: BrowserWebSocketTransport,
    audio_chunk: bytes,
    *,
    call_id: str,
    turn_id: str,
    response_id: str,
    sequence: int,
    sample_rate: int,
) -> None:
    await transport.send_audio(
        audio_chunk,
        call_id=call_id,
        turn_id=turn_id,
        response_id=response_id,
        sequence=sequence,
        sample_rate=sample_rate,
    )
