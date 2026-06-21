import asyncio
import os
from urllib.parse import urlsplit, urlunsplit

import asyncpg

from scripts.peerdb_render_flow_sql import load_env_file

BILLING_TABLES = (
    "call_usage_events",
    "billing_line_items",
    "final_call_billing_records",
    "billing_adjustments",
)


async def main() -> None:
    load_env_file()
    password = os.getenv("PEERDB_POSTGRES_PASSWORD")
    if not password or password.startswith("<"):
        raise SystemExit("PEERDB_POSTGRES_PASSWORD is required and must not be a placeholder.")

    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/voice_lab",
    )
    database_url = _host_runnable_database_url(database_url)
    connection = await asyncpg.connect(database_url)
    try:
        quoted_password = _quote_literal(password)
        create_role_sql = f"""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'voicemesh_peerdb') THEN
                    CREATE ROLE voicemesh_peerdb WITH LOGIN REPLICATION PASSWORD {quoted_password};
                ELSE
                    ALTER ROLE voicemesh_peerdb WITH LOGIN REPLICATION PASSWORD {quoted_password};
                END IF;
            END
            $$;
            """
        await connection.execute(
            create_role_sql,
        )
        await connection.execute("GRANT CONNECT ON DATABASE voice_lab TO voicemesh_peerdb")
        await connection.execute("GRANT USAGE ON SCHEMA public TO voicemesh_peerdb")
        await connection.execute(
            """
            GRANT SELECT ON TABLE
                public.call_usage_events,
                public.billing_line_items,
                public.final_call_billing_records,
                public.billing_adjustments
            TO voicemesh_peerdb
            """
        )
        exists = await connection.fetchval(
            "SELECT TRUE FROM pg_publication WHERE pubname='voicemesh_billing_publication'"
        )
        if not exists:
            await connection.execute(
                """
                CREATE PUBLICATION voicemesh_billing_publication
                FOR TABLE
                    public.call_usage_events,
                    public.billing_line_items,
                    public.final_call_billing_records,
                    public.billing_adjustments
                """
            )
        await _verify_publication(connection)
        print("Postgres billing CDC setup: OK")
    finally:
        await connection.close()


async def _verify_publication(connection: asyncpg.Connection) -> None:
    tables = {
        row["tablename"]
        for row in await connection.fetch(
            """
            SELECT schemaname, tablename
            FROM pg_publication_tables
            WHERE pubname='voicemesh_billing_publication'
            """
        )
    }
    missing = set(BILLING_TABLES) - tables
    if missing:
        raise RuntimeError(f"CDC publication is missing tables: {', '.join(sorted(missing))}")


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _host_runnable_database_url(database_url: str) -> str:
    parsed = urlsplit(database_url)
    if parsed.hostname not in {"postgres", "voicemesh-postgres-1"}:
        return database_url
    username = parsed.username or ""
    password = f":{parsed.password}" if parsed.password else ""
    port = f":{parsed.port}" if parsed.port else ":5432"
    netloc = f"{username}{password}@localhost{port}"
    return urlunsplit((parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment))


if __name__ == "__main__":
    asyncio.run(main())
