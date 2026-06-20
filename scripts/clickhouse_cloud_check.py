import asyncio

from apps.api.analytics.clickhouse.client import ClickHouseCloudClient
from apps.api.config import get_settings


async def main() -> None:
    settings = get_settings()
    if not settings.clickhouse_enabled:
        raise SystemExit("CLICKHOUSE_ENABLED=false. Enable it in .env to check Cloud connectivity.")
    client = ClickHouseCloudClient(settings, database="default")
    try:
        ok = await client.ping()
        print(f"ClickHouse Cloud connection: {'OK' if ok else 'FAILED'}")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
