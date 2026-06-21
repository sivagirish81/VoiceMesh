import asyncio
import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid5

import asyncpg

from apps.api.analytics.clickhouse.client import ClickHouseCloudClient
from apps.api.config import get_settings
from scripts.peerdb_postgres_setup import _host_runnable_database_url
from scripts.peerdb_render_flow_sql import load_env_file


async def main() -> None:
    load_env_file()
    run_id = datetime.now(UTC).strftime("cdc-%Y%m%d%H%M%S")
    connection = await asyncpg.connect(
        _host_runnable_database_url(
            os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/voice_lab")
        )
    )
    try:
        await _seed_postgres(connection, run_id)
    finally:
        await connection.close()
    print("Postgres billing records created: 4")
    await _query_clickhouse(run_id)


async def _seed_postgres(connection: asyncpg.Connection, run_id: str) -> None:
    now = datetime.now(UTC)
    calls = [
        ("normal", 120, 120_000, 220_000, "FINALIZED", []),
        ("expensive-llm", 180, 900_000, 1_300_000, "FINALIZED", []),
        ("missing-usage", 75, 50_000, 85_000, "FINALIZED_WITH_WARNINGS", ["tts_audio_seconds"]),
        ("late-adjustment", 90, 100_000, 180_000, "FINALIZED", []),
    ]
    for index, (suffix, seconds, provider_cost, charge, status, missing) in enumerate(
        calls,
        start=1,
    ):
        call_id = f"{run_id}-{suffix}"
        started_at = now - timedelta(minutes=index * 5)
        ended_at = started_at + timedelta(seconds=seconds)
        await connection.execute(
            """
            INSERT INTO calls (
                call_id, status, started_at, ended_at, current_stage,
                selected_stt_provider, selected_llm_provider, selected_tts_provider
            ) VALUES ($1,'CALL_COMPLETED',$2,$3,'billing','openai','openai','openai')
            ON CONFLICT (call_id) DO NOTHING
            """,
            call_id,
            started_at,
            ended_at,
        )
        usage_event_id = uuid5(NAMESPACE_URL, f"{call_id}:usage:llm")
        line_item_id = uuid5(NAMESPACE_URL, f"{call_id}:line-item:llm")
        await connection.execute(
            """
            INSERT INTO call_usage_events (
                event_id, tenant_id, assistant_id, call_id, turn_id, usage_type,
                component, provider, model, quantity, unit, is_estimated, is_final,
                occurred_at, trace_id, idempotency_key, metadata_json, cost_basis_json
            ) VALUES (
                $1,'cdc-demo-tenant','cdc-demo-assistant',$2,'turn-1',
                'llm_output_tokens','llm','openai','gpt-4.1-mini',$3,'token',
                false,true,$4,$5,$6,'{}'::jsonb,'{}'::jsonb
            )
            ON CONFLICT (event_id) DO NOTHING
            """,
            usage_event_id,
            call_id,
            Decimal("1000") * index,
            ended_at,
            f"trace-{call_id}",
            f"{call_id}:usage:llm",
        )
        await connection.execute(
            """
            INSERT INTO billing_line_items (
                line_item_id, usage_event_id, tenant_id, assistant_id, call_id,
                component, usage_type, quantity, unit, provider, model,
                provider_cost_microunits, customer_charge_microunits,
                currency, pricing_version, status, is_estimated, trace_id,
                idempotency_key
            ) VALUES (
                $1,$2,'cdc-demo-tenant','cdc-demo-assistant',$3,'llm',
                'llm_output_tokens',$4,'token','openai','gpt-4.1-mini',
                $5,$6,'USD','cdc-demo-v1','ACCEPTED',false,$7,$8
            )
            ON CONFLICT (line_item_id) DO NOTHING
            """,
            line_item_id,
            usage_event_id,
            call_id,
            Decimal("1000") * index,
            provider_cost,
            charge,
            f"trace-{call_id}",
            f"{call_id}:line-item:llm",
        )
        await connection.execute(
            """
            INSERT INTO final_call_billing_records (
                call_id, tenant_id, assistant_id, started_at, ended_at,
                connected_seconds, billable_seconds,
                platform_cost_microunits, llm_cost_microunits,
                provider_cost_total_microunits, customer_charge_total_microunits,
                gross_margin_microunits, currency, pricing_version, status,
                billing_status, expected_usage_components, received_usage_components,
                missing_usage_components, finalized_at, trace_id
            ) VALUES (
                $1,'cdc-demo-tenant','cdc-demo-assistant',$2,$3,$4,$4,
                $5,$6,$6,$7,$8,'USD','cdc-demo-v1',$9,$9,
                $10::jsonb,$11::jsonb,$12::jsonb,NOW(),$13
            )
            ON CONFLICT (call_id) DO UPDATE SET
                customer_charge_total_microunits=EXCLUDED.customer_charge_total_microunits,
                provider_cost_total_microunits=EXCLUDED.provider_cost_total_microunits,
                gross_margin_microunits=EXCLUDED.gross_margin_microunits,
                missing_usage_components=EXCLUDED.missing_usage_components,
                updated_at=NOW(),
                version=final_call_billing_records.version + 1
            """,
            call_id,
            started_at,
            ended_at,
            seconds,
            max(0, charge - provider_cost),
            provider_cost,
            charge,
            charge - provider_cost,
            status,
            '["stt_audio_seconds","llm_output_tokens","tts_audio_seconds"]',
            (
                '["stt_audio_seconds","llm_output_tokens"]'
                if missing
                else '["stt_audio_seconds","llm_output_tokens","tts_audio_seconds"]'
            ),
            str(missing).replace("'", '"'),
            f"trace-{call_id}",
        )
        if suffix == "late-adjustment":
            adjustment_id = uuid5(NAMESPACE_URL, f"{call_id}:adjustment")
            await connection.execute(
                """
                INSERT INTO billing_adjustments (
                    adjustment_id, original_line_item_id, call_id, tenant_id, assistant_id,
                    component, reason_code, provider_cost_delta_microunits,
                    customer_charge_delta_microunits, currency, pricing_version,
                    previous_total_cost_cents, recomputed_total_cost_cents,
                    delta_cost_cents, reason, source_event_id, status, trace_id,
                    idempotency_key
                ) VALUES (
                    $1,$2,$3,'cdc-demo-tenant','cdc-demo-assistant','tts',
                    'late_usage_after_finalization',25000,40000,'USD','cdc-demo-v1',
                    18,22,4,'late_usage_after_finalization',$4,'CREATED',$5,$6
                )
                ON CONFLICT (call_id, source_event_id) DO NOTHING
                """,
                adjustment_id,
                line_item_id,
                call_id,
                usage_event_id,
                f"trace-{call_id}",
                f"{call_id}:adjustment",
            )


async def _query_clickhouse(run_id: str) -> None:
    settings = get_settings()
    if not settings.clickhouse_enabled:
        print("ClickHouse disabled; skipping CDC validation query.")
        return
    client = ClickHouseCloudClient(settings)
    try:
        for attempt in range(1, 11):
            rows = await client.query_rows(
                f"""
                SELECT
                    count() AS calls,
                    sum(provider_cost_total_microunits) AS provider_cost,
                    sum(customer_charge_total_microunits) AS customer_charge,
                    sum(gross_margin_microunits) AS gross_margin,
                    countIf(length(missing_usage_components) > 2) AS missing_usage_calls
                FROM voicemesh.billing_calls_current
                WHERE call_id LIKE '{run_id}%'
                """
            )
            calls = rows[0][0] if rows else 0
            if calls >= 4:
                print(f"CDC rows replicated: {calls}")
                print(f"Current finalized calls: {rows[0][0]}")
                print(f"Provider cost microunits: {rows[0][1]}")
                print(f"Customer charges microunits: {rows[0][2]}")
                print(f"Gross margin microunits: {rows[0][3]}")
                print(f"Calls with missing usage: {rows[0][4]}")
                adjustments = await client.query_rows(
                    f"""
                    SELECT count(), sum(customer_charge_delta_microunits)
                    FROM voicemesh.billing_adjustments_current
                    WHERE call_id LIKE '{run_id}%'
                    """
                )
                print(f"Adjustments: {adjustments[0][0] if adjustments else 0}")
                return
            print(f"Waiting for CDC replication attempt {attempt}/10...")
            await asyncio.sleep(3)
        print("CDC rows were not visible in ClickHouse yet. Check PeerDB mirror status.")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
