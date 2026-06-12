import argparse
import asyncio
import json
import os
from uuid import uuid4

import httpx
import websockets
from openai import AsyncOpenAI


async def generate_spoken_prompt() -> bytes:
    client = AsyncOpenAI(api_key=os.environ["OPENAI_API_KEY"])
    chunks: list[bytes] = []
    async with client.audio.speech.with_streaming_response.create(
        model=os.getenv("OPENAI_TTS_MODEL", "gpt-4o-mini-tts"),
        voice=os.getenv("OPENAI_TTS_VOICE", "alloy"),
        input="Explain in two sentences why backpressure matters in a live voice pipeline.",
        response_format="pcm",
    ) as response:
        async for chunk in response.iter_bytes(chunk_size=4096):
            chunks.append(chunk)
    return b"".join(chunks)


async def run_smoke(ws_url: str, api_url: str) -> dict[str, object]:
    call_id = f"smoke-{uuid4()}"
    input_audio = await generate_spoken_prompt()
    transcript = ""
    response_parts: list[str] = []
    output_audio_bytes = 0
    observed_events: set[str] = set()

    async with websockets.connect(f"{ws_url}/ws/calls/{call_id}", max_size=16 * 1024 * 1024) as ws:
        await ws.send(json.dumps({"type": "audio.config", "sample_rate": 24000, "channels": 1}))
        for offset in range(0, len(input_audio), 4096):
            await ws.send(input_audio[offset : offset + 4096])
            await asyncio.sleep(0.02)
        await ws.send(json.dumps({"type": "audio.end_turn"}))
        await ws.send(json.dumps({"type": "call.end"}))

        async with asyncio.timeout(120):
            async for raw_message in ws:
                message = json.loads(raw_message)
                message_type = message.get("type")
                if message_type == "transcript.final":
                    transcript = message["text"]
                elif message_type == "llm.token":
                    response_parts.append(message["text"])
                elif message_type == "audio.chunk":
                    output_audio_bytes += len(message["audio"]) * 3 // 4
                elif message_type == "pipeline.event":
                    observed_events.add(message["event"]["event_type"])
                elif message_type == "error":
                    raise RuntimeError(message["message"])
                elif message_type == "call.ended":
                    break

    required_events = {
        "call.started",
        "stt.final_transcript",
        "llm.final_response",
        "tts.first_audio",
        "transport.audio_sent",
        "call.ended",
    }
    missing_events = required_events - observed_events
    if not transcript:
        raise RuntimeError("Live smoke test did not receive a final transcript")
    if not response_parts:
        raise RuntimeError("Live smoke test did not receive streamed LLM tokens")
    if output_audio_bytes == 0:
        raise RuntimeError("Live smoke test did not receive synthesized audio")
    if missing_events:
        raise RuntimeError(f"Live smoke test missed pipeline events: {sorted(missing_events)}")

    async with httpx.AsyncClient(base_url=api_url, timeout=10) as client:
        call_response = await client.get(f"/calls/{call_id}")
        call_response.raise_for_status()
        persisted_call = call_response.json()

    return {
        "call_id": call_id,
        "transcript": transcript,
        "response": "".join(response_parts),
        "output_audio_bytes": output_audio_bytes,
        "observed_event_count": len(observed_events),
        "persisted_status": persisted_call["status"],
    }


async def main() -> None:
    parser = argparse.ArgumentParser(description="Exercise the real OpenAI voice pipeline")
    parser.add_argument("--ws-url", default="ws://localhost:8000")
    parser.add_argument("--api-url", default="http://localhost:8000")
    args = parser.parse_args()
    result = await run_smoke(args.ws_url, args.api_url)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
