import asyncio
import json

import httpx


async def main() -> None:
    async with httpx.AsyncClient(base_url="http://localhost:8000", timeout=10) as client:
        health = await client.get("/health")
        health.raise_for_status()
        print(json.dumps(health.json(), indent=2))
        print("Open http://localhost:3000/demo and start the microphone.")


if __name__ == "__main__":
    asyncio.run(main())

