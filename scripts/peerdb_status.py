import asyncio
import os

import asyncpg

from apps.api.analytics.clickhouse.client import ClickHouseCloudClient
from apps.api.config import get_settings
from scripts.peerdb_postgres_setup import _host_runnable_database_url


async def main() -> None:
    await _postgres_status()
    await _clickhouse_status()


async def _postgres_status() -> None:
    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/voice_lab",
    )
    database_url = _host_runnable_database_url(database_url)
    try:
        connection = await asyncpg.connect(database_url)
    except Exception as exc:
        print(f"Postgres: unavailable ({exc.__class__.__name__})")
        return
    try:
        settings = await connection.fetch(
            """
            SELECT name, setting
            FROM pg_settings
            WHERE name IN ('wal_level', 'max_replication_slots', 'max_wal_senders')
            ORDER BY name
            """
        )
        print("Postgres logical replication settings:")
        for row in settings:
            print(f"  {row['name']}: {row['setting']}")

        publication = await connection.fetch(
            """
            SELECT tablename
            FROM pg_publication_tables
            WHERE pubname='voicemesh_billing_publication'
            ORDER BY tablename
            """
        )
        print("Publication voicemesh_billing_publication:")
        if publication:
            for row in publication:
                print(f"  {row['tablename']}")
        else:
            print("  missing")

        slots = await connection.fetch(
            """
            SELECT
                slot_name,
                plugin,
                active,
                pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint AS retained_wal_bytes
            FROM pg_replication_slots
            ORDER BY slot_name
            """
        )
        print("Replication slots:")
        if slots:
            for row in slots:
                print(
                    "  "
                    f"{row['slot_name']} plugin={row['plugin']} "
                    f"active={row['active']} retained_wal_bytes={row['retained_wal_bytes']}"
                )
        else:
            print("  none")
    finally:
        await connection.close()


async def _clickhouse_status() -> None:
    settings = get_settings()
    if not settings.clickhouse_enabled:
        print("ClickHouse: disabled")
        return
    client = ClickHouseCloudClient(settings)
    try:
        rows = await client.query_rows(
            """
            SELECT
                countIf(name = 'billing_calls_current') AS calls_view,
                countIf(name = 'billing_line_items_current') AS line_items_view,
                countIf(name = 'billing_usage_current') AS usage_view,
                countIf(name = 'billing_adjustments_current') AS adjustments_view
            FROM system.tables
            WHERE database = 'voicemesh'
            """
        )
        print(f"ClickHouse CDC views: {rows[0] if rows else 'unavailable'}")
        latest = await client.query_rows(
            """
            SELECT max(updated_at)
            FROM voicemesh.billing_calls_current
            """
        )
        latest_value = latest[0][0] if latest else None
        if str(latest_value) == "1970-01-01 00:00:00":
            latest_value = None
        print(f"Latest replicated billing update: {latest_value}")
    except Exception as exc:
        print(f"ClickHouse: unavailable or CDC views missing ({exc.__class__.__name__})")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
