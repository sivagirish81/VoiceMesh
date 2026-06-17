import asyncio
import json
import logging
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, TypeVar, cast

import asyncpg
from opentelemetry import trace

from apps.api.events.schemas import PipelineEvent, topic_for_event
from apps.api.failure_injection.injector import FailureInjector
from apps.api.telemetry.metrics import DB_WRITE_FAILURES, DUPLICATE_EVENTS

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)
T = TypeVar("T")


class PostgresRepository:
    def __init__(
        self,
        database_url: str,
        failure_injector: FailureInjector,
        min_size: int = 2,
        max_size: int = 10,
        command_timeout: float = 5.0,
    ) -> None:
        self._database_url = database_url
        self._failure_injector = failure_injector
        self._min_size = min_size
        self._max_size = max_size
        self._command_timeout = command_timeout
        self.pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        self.pool = await asyncpg.create_pool(
            self._database_url,
            min_size=self._min_size,
            max_size=self._max_size,
            command_timeout=self._command_timeout,
        )

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()
            self.pool = None

    def _require_pool(self) -> asyncpg.Pool:
        if not self.pool:
            raise RuntimeError("Postgres repository is not connected")
        return self.pool

    async def _retry(
        self, operation: Callable[[], Awaitable[T]], *, critical: bool = False
    ) -> T | None:
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                if self._failure_injector.postgres_failure:
                    raise asyncpg.PostgresConnectionError("Injected Postgres write failure")
                return await operation()
            except (asyncpg.PostgresError, OSError, TimeoutError) as exc:
                last_error = exc
                DB_WRITE_FAILURES.inc()
                logger.warning(
                    "postgres write failed: attempt=%s critical=%s error=%s",
                    attempt + 1,
                    critical,
                    exc,
                )
                await asyncio.sleep(0.1 * (2**attempt))
        if critical and last_error:
            raise last_error
        return None

    async def create_call(
        self, call_id: str, stt_provider: str, llm_provider: str, tts_provider: str
    ) -> bool:
        async def operation() -> str:
            return cast(
                str,
                await self._require_pool().execute(
                    """
                    INSERT INTO calls (
                        call_id, status, started_at, selected_stt_provider,
                        selected_llm_provider, selected_tts_provider
                    ) VALUES ($1, 'CALL_STARTED', NOW(), $2, $3, $4)
                    ON CONFLICT (call_id) DO NOTHING
                    """,
                    call_id,
                    stt_provider,
                    llm_provider,
                    tts_provider,
                ),
            )

        result = await self._retry(operation)
        return result == "INSERT 0 1"

    async def persist_event(
        self, event: PipelineEvent, *, critical: bool = False
    ) -> bool | None:
        async def operation() -> bool:
            pool = self._require_pool()
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
                await connection.execute(
                    """
                    INSERT INTO call_events (
                        event_id, call_id, turn_id, event_type, stage,
                        sequence_number, idempotency_key, payload, trace_id, created_at
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10)
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
                if critical:
                    await connection.execute(
                        """
                        INSERT INTO outbox_events (event_id, topic, key, payload)
                        VALUES ($1, $2, $3, $4::jsonb)
                        ON CONFLICT (event_id) DO NOTHING
                        """,
                        event.event_id,
                        topic_for_event(event.event_type),
                        event.call_id,
                        event.model_dump_json(),
                    )
                return True

        with tracer.start_as_current_span("postgres.persist_event") as span:
            span.set_attribute("call_id", event.call_id)
            span.set_attribute("idempotency_key", event.idempotency_key)
            return await self._retry(operation, critical=critical)

    async def update_call_state(
        self,
        call_id: str,
        *,
        status: str | None = None,
        stage: str | None = None,
        corked: bool | None = None,
        cork_reason: str | None = None,
        final_summary: str | None = None,
        error: str | None = None,
        ended: bool = False,
    ) -> None:
        async def operation() -> None:
            await self._require_pool().execute(
                """
                UPDATE calls SET
                    status = COALESCE($2, status),
                    current_stage = COALESCE($3, current_stage),
                    corked = COALESCE($4, corked),
                    cork_reason = CASE WHEN $4 IS NULL THEN cork_reason ELSE $5 END,
                    final_summary = COALESCE($6, final_summary),
                    error = COALESCE($7, error),
                    ended_at = CASE WHEN $8 THEN NOW() ELSE ended_at END,
                    updated_at = NOW()
                WHERE call_id = $1
                """,
                call_id,
                status,
                stage,
                corked,
                cork_reason,
                final_summary,
                error,
                ended,
            )

        await self._retry(operation)

    async def record_metric(
        self,
        call_id: str,
        turn_id: str,
        stage: str,
        latency_ms: float,
        queue_depth: int,
        corked: bool,
        provider: str | None,
    ) -> None:
        async def operation() -> None:
            await self._require_pool().execute(
                """
                INSERT INTO pipeline_metrics (
                    call_id, turn_id, stage, latency_ms, queue_depth, corked, provider
                ) VALUES ($1,$2,$3,$4,$5,$6,$7)
                """,
                call_id,
                turn_id,
                stage,
                latency_ms,
                queue_depth,
                corked,
                provider,
            )

        await self._retry(operation)

    async def list_calls(self, limit: int = 100) -> list[dict[str, Any]]:
        rows = await self._require_pool().fetch(
            "SELECT * FROM calls ORDER BY created_at DESC LIMIT $1", limit
        )
        return [dict(row) for row in rows]

    async def get_call(self, call_id: str) -> dict[str, Any] | None:
        row = await self._require_pool().fetchrow("SELECT * FROM calls WHERE call_id=$1", call_id)
        return dict(row) if row else None

    async def get_events(self, call_id: str) -> list[dict[str, Any]]:
        rows = await self._require_pool().fetch(
            "SELECT * FROM call_events WHERE call_id=$1 ORDER BY created_at, sequence_number",
            call_id,
        )
        events = [dict(row) for row in rows]
        for event in events:
            if isinstance(event["payload"], str):
                event["payload"] = json.loads(event["payload"])
        return events

    async def get_metrics(self, call_id: str) -> list[dict[str, Any]]:
        rows = await self._require_pool().fetch(
            "SELECT * FROM pipeline_metrics WHERE call_id=$1 ORDER BY created_at", call_id
        )
        return [dict(row) for row in rows]

    async def metrics_summary(self) -> Sequence[asyncpg.Record]:
        return cast(
            Sequence[asyncpg.Record],
            await self._require_pool().fetch(
                """
                SELECT stage,
                       percentile_cont(0.50) WITHIN GROUP (ORDER BY latency_ms) AS p50,
                       percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95,
                       percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_ms) AS p99,
                       COUNT(*) AS samples
                FROM pipeline_metrics
                GROUP BY stage
                ORDER BY stage
                """
            ),
        )

    async def billing_summary(self) -> dict[str, Any]:
        pool = self._require_pool()
        totals = await pool.fetchrow(
            """
            SELECT
                COALESCE(COUNT(f.call_id), 0) AS finalized_calls,
                COALESCE(SUM(f.billable_seconds), 0) AS finalized_duration_seconds,
                COALESCE(SUM(f.platform_cost_cents), 0) AS platform_cost_cents,
                COALESCE(SUM(f.stt_cost_cents), 0) AS stt_cost_cents,
                COALESCE(SUM(f.llm_cost_cents), 0) AS llm_cost_cents,
                COALESCE(SUM(f.tts_cost_cents), 0) AS tts_cost_cents,
                COALESCE(SUM(f.telephony_cost_cents), 0) AS telephony_cost_cents,
                COALESCE(SUM(f.total_cost_cents), 0) AS total_cost_cents
            FROM final_call_billing_records f
            """
        )
        live_totals = await pool.fetchrow(
            """
            SELECT
                COUNT(*) AS calls,
                COALESCE(SUM(call_duration_seconds), 0) AS duration_seconds,
                COALESCE(SUM(provider_cost_usd), 0) AS provider_cost_usd,
                COALESCE(SUM(platform_fee_usd), 0) AS platform_fee_usd,
                COALESCE(SUM(total_cost_usd), 0) AS total_cost_usd
            FROM call_billing
            """
        )
        stages = await pool.fetch(
            """
            SELECT stage, provider, model, usage_type, unit,
                SUM(quantity) AS quantity,
                SUM(cost_usd) AS cost_usd,
                BOOL_OR(estimated) AS has_estimates
            FROM usage_records
            GROUP BY stage, provider, model, usage_type, unit
            ORDER BY stage, usage_type
            """
        )
        return {
            "totals": {
                **(dict(live_totals) if live_totals else {}),
                **(dict(totals) if totals else {}),
            },
            "usage": [dict(row) for row in stages],
        }

    async def list_billing_calls(self, limit: int = 100) -> list[dict[str, Any]]:
        rows = await self._require_pool().fetch(
            """
            SELECT
                COALESCE(f.call_id, b.call_id) AS call_id,
                COALESCE(f.billable_seconds, b.call_duration_seconds) AS call_duration_seconds,
                COALESCE(
                    (f.stt_cost_cents + f.llm_cost_cents + f.tts_cost_cents
                        + f.telephony_cost_cents)::numeric / 100,
                    b.provider_cost_usd,
                    0
                ) AS provider_cost_usd,
                COALESCE(f.platform_cost_cents::numeric / 100, b.platform_fee_usd, 0)
                    AS platform_fee_usd,
                COALESCE(f.total_cost_cents::numeric / 100, b.total_cost_usd, 0)
                    AS total_cost_usd,
                COALESCE(f.currency, b.currency, 'USD') AS currency,
                COALESCE(f.status, b.status) AS status,
                COALESCE(f.pricing_version, b.pricing_version) AS pricing_version,
                COALESCE(f.updated_at, b.updated_at) AS updated_at,
                f.platform_cost_cents, f.stt_cost_cents, f.llm_cost_cents,
                f.tts_cost_cents, f.telephony_cost_cents, f.total_cost_cents,
                c.status AS call_status, c.started_at, c.ended_at,
                c.selected_stt_model, c.selected_llm_model, c.selected_tts_model
            FROM call_billing b
            FULL OUTER JOIN final_call_billing_records f USING (call_id)
            LEFT JOIN calls c USING (call_id)
            ORDER BY updated_at DESC
            LIMIT $1
            """,
            limit,
        )
        return [dict(row) for row in rows]

    async def get_call_billing(self, call_id: str) -> dict[str, Any] | None:
        pool = self._require_pool()
        billing = await pool.fetchrow(
            """
            SELECT
                COALESCE(f.call_id, b.call_id) AS call_id,
                COALESCE(f.billable_seconds, b.call_duration_seconds) AS call_duration_seconds,
                COALESCE(
                    (f.stt_cost_cents + f.llm_cost_cents + f.tts_cost_cents
                        + f.telephony_cost_cents)::numeric / 100,
                    b.provider_cost_usd,
                    0
                ) AS provider_cost_usd,
                COALESCE(f.platform_cost_cents::numeric / 100, b.platform_fee_usd, 0)
                    AS platform_fee_usd,
                COALESCE(f.total_cost_cents::numeric / 100, b.total_cost_usd, 0)
                    AS total_cost_usd,
                COALESCE(f.currency, b.currency, 'USD') AS currency,
                COALESCE(f.status, b.status) AS status,
                COALESCE(f.pricing_version, b.pricing_version) AS pricing_version,
                COALESCE(f.updated_at, b.updated_at) AS updated_at,
                f.platform_cost_cents, f.stt_cost_cents, f.llm_cost_cents,
                f.tts_cost_cents, f.telephony_cost_cents, f.total_cost_cents,
                f.warnings, f.finalized_at,
                c.status AS call_status, c.started_at, c.ended_at,
                c.selected_stt_model, c.selected_llm_model, c.selected_tts_model
            FROM call_billing b
            FULL OUTER JOIN final_call_billing_records f USING (call_id)
            LEFT JOIN calls c USING (call_id)
            WHERE COALESCE(f.call_id, b.call_id)=$1
            """,
            call_id,
        )
        if not billing:
            return None
        usage = await pool.fetch(
            """
            SELECT * FROM usage_records
            WHERE call_id=$1
            ORDER BY created_at, id
            """,
            call_id,
        )
        return {"billing": dict(billing), "usage": [dict(row) for row in usage]}

    async def health(self) -> bool:
        try:
            return bool(await self._require_pool().fetchval("SELECT TRUE"))
        except Exception:
            return False

    async def reset_demo(self) -> None:
        await self._require_pool().execute(
            """
            TRUNCATE webhook_delivery_attempts, webhook_deliveries,
                tool_invocation_attempts, tool_invocations,
                final_call_billing_records, call_usage_rollups, call_usage_events,
                usage_records, call_billing, call_events, idempotency_keys,
                outbox_events, pipeline_metrics, calls
            """
        )
