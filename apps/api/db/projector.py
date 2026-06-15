import json
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid5

import asyncpg

from apps.api.db.repository import PostgresRepository
from apps.api.events.schemas import EventType, PipelineEvent
from apps.api.telemetry.metrics import DUPLICATE_EVENTS


class EventProjector:
    def __init__(
        self,
        repository: PostgresRepository,
        *,
        platform_rate_per_minute_usd: float,
        billing_pricing_version: str,
    ) -> None:
        self._repository = repository
        self._platform_rate = Decimal(str(platform_rate_per_minute_usd))
        self._billing_pricing_version = billing_pricing_version

    async def project(self, event: PipelineEvent) -> bool | None:
        async def operation() -> bool:
            pool = self._repository._require_pool()
            async with pool.acquire() as connection, connection.transaction():
                inserted = await connection.fetchval(
                    """
                    INSERT INTO idempotency_keys (idempotency_key, call_id, event_type)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (idempotency_key) DO NOTHING
                    RETURNING TRUE
                    """,
                    event.idempotency_key,
                    event.call_id,
                    str(event.event_type),
                )
                if not inserted:
                    DUPLICATE_EVENTS.labels(str(event.event_type)).inc()
                    return False

                await self._insert_event(connection, event)
                await self._project_call(connection, event)
                await self._project_metric(connection, event)
                await self._project_usage(connection, event)
                return True

        return await self._repository._retry(operation, critical=True)

    async def _insert_event(
        self, connection: asyncpg.Connection, event: PipelineEvent
    ) -> None:
        await connection.execute(
            """
            INSERT INTO call_events (
                event_id, call_id, turn_id, event_type, stage,
                sequence_number, idempotency_key, payload, trace_id, created_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10)
            ON CONFLICT (event_id) DO NOTHING
            """,
            event.event_id,
            event.call_id,
            event.turn_id,
            str(event.event_type),
            event.stage,
            event.sequence_number,
            event.idempotency_key,
            json.dumps(event.payload),
            event.trace_id,
            event.timestamp,
        )

    async def _project_call(
        self, connection: asyncpg.Connection, event: PipelineEvent
    ) -> None:
        event_type = str(event.event_type)
        if event.event_type == EventType.BILLING_USAGE_RECORDED:
            return
        if event.event_type == EventType.CALL_STARTED:
            providers = event.payload.get("providers", {})
            models = event.payload.get("models", {})
            await connection.execute(
                """
                INSERT INTO calls (
                    call_id, status, started_at, current_stage,
                    selected_stt_provider, selected_llm_provider, selected_tts_provider,
                    selected_stt_model, selected_llm_model, selected_tts_model
                ) VALUES ($1, 'CALL_STARTED', $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (call_id) DO UPDATE SET
                    status='CALL_STARTED',
                    started_at=COALESCE(calls.started_at, EXCLUDED.started_at),
                    selected_stt_provider=EXCLUDED.selected_stt_provider,
                    selected_llm_provider=EXCLUDED.selected_llm_provider,
                    selected_tts_provider=EXCLUDED.selected_tts_provider,
                    selected_stt_model=EXCLUDED.selected_stt_model,
                    selected_llm_model=EXCLUDED.selected_llm_model,
                    selected_tts_model=EXCLUDED.selected_tts_model,
                    updated_at=NOW()
                """,
                event.call_id,
                event.timestamp,
                event.stage,
                providers.get("stt", "unknown"),
                providers.get("llm", "unknown"),
                providers.get("tts", "unknown"),
                models.get("stt"),
                models.get("llm"),
                models.get("tts"),
            )
        elif event.event_type == EventType.CALL_ENDED:
            await connection.execute(
                """
                UPDATE calls SET status='CALL_COMPLETED', current_stage=$2,
                    final_summary=$3, ended_at=$4, updated_at=NOW()
                WHERE call_id=$1
                """,
                event.call_id,
                event.stage,
                event.payload.get("final_response"),
                event.timestamp,
            )
            await self._update_call_billing(
                connection,
                event.call_id,
                duration_seconds=Decimal(str(event.payload.get("duration_seconds", 0))),
                finalized=True,
            )
        elif event.event_type == EventType.CALL_FAILED:
            await connection.execute(
                """
                UPDATE calls SET status='CALL_FAILED', current_stage=$2,
                    error=$3, ended_at=$4, updated_at=NOW()
                WHERE call_id=$1
                """,
                event.call_id,
                event.stage,
                event.payload.get("error"),
                event.timestamp,
            )
            await self._update_call_billing(
                connection, event.call_id, duration_seconds=None, finalized=True
            )
        else:
            corked: bool | None = None
            cork_reason: str | None = None
            if event.event_type == EventType.PIPELINE_CORKED:
                corked = True
                cork_reason = str(event.payload.get("reason") or "")
            elif event.event_type == EventType.PIPELINE_UNCORKED:
                corked = False
            await connection.execute(
                """
                UPDATE calls SET
                    current_stage=$2,
                    corked=COALESCE($3, corked),
                    cork_reason=CASE WHEN $3 IS NULL THEN cork_reason ELSE $4 END,
                    status=CASE
                        WHEN status='CALL_STARTED' AND $5 NOT LIKE 'call.%'
                        THEN 'IN_PROGRESS'
                        ELSE status
                    END,
                    updated_at=NOW()
                WHERE call_id=$1
                """,
                event.call_id,
                event.stage,
                corked,
                cork_reason,
                event_type,
            )

    async def _project_metric(
        self, connection: asyncpg.Connection, event: PipelineEvent
    ) -> None:
        latency = event.payload.get("latency_ms")
        if not isinstance(latency, int | float):
            return
        await connection.execute(
            """
            INSERT INTO pipeline_metrics (
                call_id, turn_id, stage, latency_ms, queue_depth, corked, provider
            ) VALUES ($1,$2,$3,$4,$5,$6,$7)
            """,
            event.call_id,
            event.turn_id,
            event.stage,
            float(latency),
            int(event.payload.get("queue_depth", 0)),
            event.event_type == EventType.PIPELINE_CORKED,
            event.payload.get("provider"),
        )

    async def _project_usage(
        self, connection: asyncpg.Connection, event: PipelineEvent
    ) -> None:
        if not str(event.event_type).startswith("usage."):
            return
        provider = str(event.payload["provider"])
        model = str(event.payload["model"])
        measurements = event.payload.get("measurements", [])
        for measurement in measurements:
            usage_type = str(measurement["usage_type"])
            quantity = Decimal(str(measurement["quantity"]))
            price = await connection.fetchrow(
                """
                SELECT unit, unit_price_usd, pricing_version
                FROM pricing_catalog
                WHERE provider=$1 AND model=$2 AND usage_type=$3
                ORDER BY effective_at DESC LIMIT 1
                """,
                provider,
                model,
                usage_type,
            )
            if not price:
                raise ValueError(f"No price configured for {provider}/{model}/{usage_type}")
            cost = quantity * price["unit_price_usd"]
            await connection.execute(
                """
                INSERT INTO usage_records (
                    event_id, call_id, turn_id, stage, provider, model,
                    usage_type, quantity, unit, unit_price_usd, cost_usd,
                    estimated, pricing_version, metadata, created_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14::jsonb,$15)
                ON CONFLICT (event_id, usage_type) DO NOTHING
                """,
                event.event_id,
                event.call_id,
                event.turn_id,
                event.stage,
                provider,
                model,
                usage_type,
                quantity,
                price["unit"],
                price["unit_price_usd"],
                cost,
                bool(measurement.get("estimated", False)),
                price["pricing_version"],
                json.dumps(
                    {
                        key: value
                        for key, value in event.payload.items()
                        if key not in {"provider", "model", "measurements"}
                    }
                ),
                event.timestamp,
            )

        billing = await self._update_call_billing(
            connection, event.call_id, duration_seconds=None, finalized=False
        )
        await self._enqueue_billing_event(connection, event, billing)

    async def _update_call_billing(
        self,
        connection: asyncpg.Connection,
        call_id: str,
        *,
        duration_seconds: Decimal | None,
        finalized: bool,
    ) -> asyncpg.Record:
        await connection.execute(
            """
            INSERT INTO call_billing (call_id, pricing_version)
            VALUES ($1, $2)
            ON CONFLICT (call_id) DO NOTHING
            """,
            call_id,
            self._billing_pricing_version,
        )
        if duration_seconds is not None:
            await connection.execute(
                """
                UPDATE call_billing
                SET call_duration_seconds=$2::numeric,
                    platform_fee_usd=($2::numeric / 60::numeric) * $3::numeric,
                    status=CASE WHEN $4 THEN 'FINALIZED' ELSE status END,
                    finalized_at=CASE WHEN $4 THEN NOW() ELSE finalized_at END,
                    updated_at=NOW()
                WHERE call_id=$1
                """,
                call_id,
                duration_seconds,
                self._platform_rate,
                finalized,
            )
        elif finalized:
            await connection.execute(
                """
                UPDATE call_billing SET status='FINALIZED',
                    finalized_at=NOW(), updated_at=NOW()
                WHERE call_id=$1
                """,
                call_id,
            )
        billing = await connection.fetchrow(
            """
            UPDATE call_billing SET
                provider_cost_usd=COALESCE((
                    SELECT SUM(cost_usd) FROM usage_records WHERE call_id=$1
                ), 0),
                total_cost_usd=platform_fee_usd + COALESCE((
                    SELECT SUM(cost_usd) FROM usage_records WHERE call_id=$1
                ), 0),
                updated_at=NOW()
            WHERE call_id=$1
            RETURNING *
            """,
            call_id,
        )
        if not billing:
            raise RuntimeError(f"Billing rollup missing for call {call_id}")
        return billing

    async def _enqueue_billing_event(
        self,
        connection: asyncpg.Connection,
        source: PipelineEvent,
        billing: asyncpg.Record,
    ) -> None:
        event_id = uuid5(NAMESPACE_URL, f"voicemesh:billing:{source.event_id}")
        event = PipelineEvent(
            event_id=event_id,
            call_id=source.call_id,
            turn_id=source.turn_id,
            event_type=EventType.BILLING_USAGE_RECORDED,
            stage="billing",
            timestamp=source.timestamp,
            sequence_number=source.sequence_number,
            idempotency_key=f"billing:{source.event_id}",
            payload={
                "source_event_id": str(source.event_id),
                "provider_cost_usd": str(billing["provider_cost_usd"]),
                "platform_fee_usd": str(billing["platform_fee_usd"]),
                "total_cost_usd": str(billing["total_cost_usd"]),
                "currency": billing["currency"],
                "status": billing["status"],
            },
            trace_id=source.trace_id,
        )
        await connection.execute(
            """
            INSERT INTO outbox_events (event_id, topic, key, payload)
            VALUES ($1, 'billing-events', $2, $3::jsonb)
            ON CONFLICT (event_id) DO NOTHING
            """,
            event.event_id,
            event.call_id,
            event.model_dump_json(),
        )
