import asyncio
import json

import httpx


async def main() -> None:
    async with httpx.AsyncClient(base_url="http://localhost:8000", timeout=10) as client:
        calls_response = await client.get("/calls")
        calls_response.raise_for_status()
        calls = calls_response.json()
        if not calls:
            raise SystemExit("No calls exist. Complete a browser call first.")
        call_id = calls[0]["call_id"]
        response = await client.post(f"/demo/replay-duplicate-events/{call_id}")
        response.raise_for_status()
        print(json.dumps(response.json(), indent=2))


if __name__ == "__main__":
    asyncio.run(main())

