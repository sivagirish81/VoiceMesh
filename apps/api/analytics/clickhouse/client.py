import asyncio
from collections.abc import Sequence
from typing import Any

import clickhouse_connect

from apps.api.analytics.clickhouse.models import CLICKHOUSE_COLUMNS
from apps.api.config import Settings


class ClickHouseCloudClient:
    def __init__(self, settings: Settings, *, database: str | None = None) -> None:
        self._settings = settings
        self._database = database or settings.clickhouse_database
        self._client: Any | None = None

    def _get_client(self) -> Any:
        if self._client is None:
            self._client = clickhouse_connect.get_client(
                host=self._settings.clickhouse_host,
                port=self._settings.clickhouse_port,
                username=self._settings.clickhouse_writer_user,
                password=self._settings.clickhouse_writer_password,
                database=self._database,
                secure=self._settings.clickhouse_secure,
                verify=self._settings.clickhouse_verify_tls,
                connect_timeout=self._settings.clickhouse_connect_timeout_seconds,
                send_receive_timeout=self._settings.clickhouse_send_receive_timeout_seconds,
                client_name="voicemesh-clickhouse-writer",
            )
        return self._client

    async def ping(self) -> bool:
        def _ping() -> bool:
            client = self._get_client()
            result = client.query("SELECT 1").result_rows
            return bool(result and result[0][0] == 1)

        return await asyncio.to_thread(_ping)

    async def insert_voice_events(self, rows: Sequence[Sequence[object]]) -> None:
        if not rows:
            return

        def _insert() -> None:
            self._get_client().insert(
                "voice_events",
                list(rows),
                column_names=CLICKHOUSE_COLUMNS,
            )

        await asyncio.to_thread(_insert)

    async def command(self, sql: str) -> None:
        await asyncio.to_thread(self._get_client().command, sql)

    async def query_rows(self, sql: str) -> list[tuple[Any, ...]]:
        def _query() -> list[tuple[Any, ...]]:
            return list(self._get_client().query(sql).result_rows)

        return await asyncio.to_thread(_query)

    async def close(self) -> None:
        client = self._client
        if client is None:
            return
        await asyncio.to_thread(client.close)
        self._client = None
