import os
from uuid import uuid4

import pytest

from apps.api.analytics.clickhouse.client import ClickHouseCloudClient
from apps.api.analytics.clickhouse.normalizer import normalize_pipeline_event
from apps.api.analytics.clickhouse.writer import ClickHouseBatchWriter
from apps.api.config import Settings
from apps.api.events.schemas import EventType, PipelineEvent
from scripts.clickhouse_cloud_bootstrap import BOOTSTRAP_SQL, _statements


@pytest.mark.skipif(
    os.getenv("CLICKHOUSE_RUN_INTEGRATION") != "1",
    reason="set CLICKHOUSE_RUN_INTEGRATION=1 to run ClickHouse Cloud integration tests",
)
async def test_clickhouse_cloud_insert_and_query() -> None:
    settings = Settings()
    run_id = f"test-{uuid4().hex}"
    bootstrap_client = ClickHouseCloudClient(settings, database="default")
    client: ClickHouseCloudClient | None = None
    try:
        for statement in _statements(BOOTSTRAP_SQL.read_text()):
            await bootstrap_client.command(statement)
        await bootstrap_client.close()
        client = ClickHouseCloudClient(settings)
        writer = ClickHouseBatchWriter(client, max_rows=10)
        event = PipelineEvent.create(
            call_id=f"call-{run_id}",
            turn_id="turn-1",
            event_type=EventType.CALL_STARTED,
            stage="call",
            sequence_number=1,
            payload={"tenant_id": run_id, "assistant_id": "integration-test"},
        )
        await writer.add(normalize_pipeline_event(event))
        await writer.flush()

        rows = await client.query_rows(
            f"SELECT uniqExact(event_id) FROM voice_events WHERE tenant_id = '{run_id}'"
        )
        assert rows[0][0] == 1
    finally:
        await bootstrap_client.close()
        if client is not None:
            await client.close()
