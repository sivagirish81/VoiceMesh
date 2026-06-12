import asyncio
import json
import time
from typing import Any

import httpx
from smoke_live_pipeline import run_smoke

TTS_DELAY_MS = 400
RECOVERY_AFTER_SECONDS = 4


async def wait_for_api(client: httpx.AsyncClient) -> None:
    for _ in range(30):
        try:
            response = await client.get("/health")
            if response.is_success:
                return
        except httpx.HTTPError:
            pass
        await asyncio.sleep(1)
    raise RuntimeError("VoiceMesh API did not become ready within 30 seconds")


async def main() -> None:
    async with httpx.AsyncClient(base_url="http://localhost:8000", timeout=10) as client:
        await wait_for_api(client)
        await client.post(
            "/demo/failure-injection",
            json={"enabled": False, "tts_delay_ms": 0},
        )
        response = await client.post(
            "/demo/failure-injection",
            json={"enabled": True, "tts_delay_ms": TTS_DELAY_MS},
        )
        response.raise_for_status()
        print(f"Armed {TTS_DELAY_MS} ms TTS delay per audio chunk.")

        recovery_task: asyncio.Task[None] | None = None
        corked_at: float | None = None
        recovered_at: float | None = None
        recovery_triggered = False
        last_depth = -1

        async def recover() -> None:
            nonlocal recovered_at
            await asyncio.sleep(RECOVERY_AFTER_SECONDS)
            reset = await client.post(
                "/demo/failure-injection",
                json={"enabled": True, "tts_delay_ms": 0},
            )
            reset.raise_for_status()
            recovered_at = time.monotonic()
            print(f"RECOVERY: removed TTS delay after {RECOVERY_AFTER_SECONDS} seconds.")

        async def observe(message: dict[str, Any]) -> None:
            nonlocal corked_at, last_depth, recovery_task, recovery_triggered
            if message.get("type") != "pipeline.event":
                return
            event = message["event"]
            event_type = event["event_type"]
            payload = event.get("payload", {})
            depth = payload.get("queue_depth")
            if event_type == "llm.token" and isinstance(depth, int) and depth != last_depth:
                last_depth = depth
                print(f"QUEUE: llm_to_tts depth={depth}")
            elif event_type == "pipeline.corked":
                corked_at = time.monotonic()
                print(f"CORKED: {payload['reason']}")
                if not recovery_triggered:
                    recovery_triggered = True
                    recovery_task = asyncio.create_task(recover())
            elif event_type == "pipeline.uncorked":
                duration = time.monotonic() - corked_at if corked_at else 0
                print(f"UNCORKED after {duration:.1f}s: {payload['reason']}")
            elif event_type in {
                "stt.final_transcript",
                "llm.final_response",
                "tts.first_audio",
                "call.ended",
            }:
                print(f"EVENT: {event_type}")

        try:
            result = await run_smoke(
                "ws://localhost:8000",
                "http://localhost:8000",
                prompt=(
                    "Give a detailed two-sentence explanation of how a production voice AI "
                    "system should react when text generation outruns speech synthesis."
                ),
                observer=observe,
            )
            if recovery_task:
                await recovery_task
        finally:
            await client.post(
                "/demo/failure-injection",
                json={"enabled": False, "tts_delay_ms": 0},
            )

        if result["cork_count"] == 0 or result["uncork_count"] == 0:
            raise RuntimeError("Expected at least one cork and uncork transition")
        if recovered_at is None:
            raise RuntimeError("Recovery was never triggered")

        print("\nBackpressure demo completed:")
        print(json.dumps(result, indent=2))
        call_id = result["call_id"]
        print(f"\nCall timeline: http://localhost:3000/calls/{call_id}")
        if result["trace_id"]:
            print(f"Jaeger trace: http://localhost:16686/trace/{result['trace_id']}")


if __name__ == "__main__":
    asyncio.run(main())
