import asyncio
from pathlib import Path

from apps.api.analytics.clickhouse.client import ClickHouseCloudClient
from apps.api.config import get_settings
from scripts.clickhouse_cloud_bootstrap import _statements

CDC_BOOTSTRAP_SQL = Path("infra/clickhouse/cloud/cdc-bootstrap.sql.template")


async def main() -> None:
    settings = get_settings()
    if not settings.clickhouse_enabled:
        raise SystemExit(
            "CLICKHOUSE_ENABLED=false. Add ClickHouse Cloud credentials to .env first."
        )
    client = ClickHouseCloudClient(settings, database="default")
    try:
        sql = await asyncio.to_thread(CDC_BOOTSTRAP_SQL.read_text)
        for statement in _statements(sql):
            await client.command(statement)
        print("ClickHouse billing CDC schema/views bootstrap: OK")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
