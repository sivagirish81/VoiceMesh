import asyncio
from pathlib import Path

from apps.api.analytics.clickhouse.client import ClickHouseCloudClient
from apps.api.config import get_settings

BOOTSTRAP_SQL = Path("infra/clickhouse/cloud/bootstrap.sql.template")


async def main() -> None:
    settings = get_settings()
    if not settings.clickhouse_enabled:
        raise SystemExit("CLICKHOUSE_ENABLED=false. Enable it in .env before bootstrapping.")
    client = ClickHouseCloudClient(settings, database="default")
    try:
        sql = await asyncio.to_thread(BOOTSTRAP_SQL.read_text)
        for statement in _statements(sql):
            await client.command(statement)
        print("ClickHouse Cloud schema bootstrap: OK")
    finally:
        await client.close()


def _statements(sql: str) -> list[str]:
    cleaned_lines = [
        line
        for line in sql.splitlines()
        if line.strip() and not line.lstrip().startswith("--")
    ]
    cleaned = "\n".join(cleaned_lines)
    return [statement.strip() for statement in cleaned.split(";") if statement.strip()]


if __name__ == "__main__":
    asyncio.run(main())
