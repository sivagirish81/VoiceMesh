import asyncio
import json

import httpx


async def main() -> None:
    async with httpx.AsyncClient(base_url="http://localhost:8000", timeout=10) as client:
        response = await client.post(
            "/demo/failure-injection",
            json={"enabled": True, "tts_delay_ms": 1500},
        )
        response.raise_for_status()
        print(json.dumps(response.json(), indent=2))
        print("TTS delay is armed. Speak a prompt that invites a multi-sentence answer.")
        print("Reset from the dashboard or POST tts_delay_ms=0 after observing cork/uncork.")


if __name__ == "__main__":
    asyncio.run(main())

