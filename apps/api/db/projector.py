import json
import time
from dataclasses import dataclass, field
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid5

import asyncpg
from opentelemetry import trace

from apps.api.db.repository import PostgresRepository
from apps.api.events.kafka_consumer import ConsumedEvent
from apps.api.events.schemas import EventType, PipelineEvent
from apps.api.telemetry.metrics import (
    CALL_EVENTS_TOTAL,
    DUPLICATE_EVENTS,
    POSTGRES_PROJECTED_EVENTS_TOTAL,
    POSTGRES_PROJECTION_DURATION,
    POSTGRES_PROJECTION_ERRORS_TOTAL,
)
from apps.api.telemetry.tracing import set_span_attributes

tracer = trace.get_tracer(__name__)


@dataclass
class ProjectionBatchResult:
    inserted_events: list[PipelineEvent] = field(default_factory=list)
    duplicate_events: list[PipelineEvent] = field(default_factory=list)
    usage_projection_call_ids: set[str] = field(default_factory=set)
    late_usage_call_ids: set[str] = field(default_factory=set)
    late_usage_events: list[PipelineEvent] = field(default_factory=list)
    call_ended_events: list[PipelineEvent] = field(default_factory=list)


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
                return await self._project_one(connection, event)

        with tracer.start_as_current_span("postgres.project_event") as span:
            set_span_attributes(
                span,
                call_id=event.call_id,
                turn_id=event.turn_id,
                event_id=str(event.event_id),
                event_type=str(event.event_type),
                stage=event.stage,
                sequence_number=event.sequence_number,
                idempotency_key=event.idempotency_key,
            )
            result = await self._repository._retry(operation, critical=True)
            set_span_attributes(span, duplicate_event=result is False)
            return result

    async def project_batch(
        self,
        consumed_events: list[ConsumedEvent],
        *,
        consumer_group: str,
    ) -> ProjectionBatchResult | None:
        async def operation() -> ProjectionBatchResult:
            result = ProjectionBatchResult()
            pool = self._repository._require_pool()
            async with pool.acquire() as connection, connection.transaction():
                watermarks: dict[tuple[str, int], int] = {}
                for item in consumed_events:
                    event = item.event
                    inserted = await self._project_one(
                        connection,
                        event,
                        topic=item.topic,
                        partition=item.partition,
                        offset=item.offset,
                    )
                    watermarks[(item.topic, item.partition)] = max(
                        watermarks.get((item.topic, item.partition), -1),
                        item.offset,
                    )
                    if inserted:
                        result.inserted_events.append(event)
                        if event.event_type == EventType.CALL_ENDED:
                            result.call_ended_events.append(event)
                        if str(event.event_type).startswith("usage."):
                            result.usage_projection_call_ids.add(event.call_id)
                            if event.event_type != EventType.USAGE_FINALIZATION_BARRIER:
                                finalized = await connection.fetchval(
                                    """
                                    SELECT TRUE FROM final_call_billing_records
                                    WHERE call_id=$1
                                    """,
                                    event.call_id,
                                )
                                if finalized:
                                    result.late_usage_call_ids.add(event.call_id)
                                    result.late_usage_events.append(event)
                    else:
                        result.duplicate_events.append(event)

                for (topic, partition), offset in watermarks.items():
                    await connection.execute(
                        """
                        INSERT INTO projection_watermarks (
                            consumer_group, topic, partition, last_projected_offset
                        ) VALUES ($1,$2,$3,$4)
                        ON CONFLICT (consumer_group, topic, partition) DO UPDATE SET
                            last_projected_offset=GREATEST(
                                projection_watermarks.last_projected_offset,
                                EXCLUDED.last_projected_offset
                            ),
                            updated_at=NOW()
                        """,
                        consumer_group,
                        topic,
                        partition,
                        offset,
                    )
                return result

        with tracer.start_as_current_span("postgres.project_batch") as span:
            started = time.perf_counter()
            set_span_attributes(
                span,
                batch_size=len(consumed_events),
                consumer_group=consumer_group,
            )
            result = await self._repository._retry(operation, critical=True)
            if result:
                POSTGRES_PROJECTION_DURATION.labels("batch").observe(
                    time.perf_counter() - started
                )
                for event in result.inserted_events:
                    POSTGRES_PROJECTED_EVENTS_TOTAL.labels(str(event.event_type)).inc()
                    if event.event_type in {
                        EventType.CALL_STARTED,
                        EventType.CALL_ENDED,
                        EventType.CALL_FAILED,
                    }:
                        CALL_EVENTS_TOTAL.labels(str(event.event_type)).inc()
                set_span_attributes(
                    span,
                    inserted_count=len(result.inserted_events),
                    duplicate_count=len(result.duplicate_events),
                    affected_usage_calls=len(result.usage_projection_call_ids),
                    late_usage_calls=len(result.late_usage_call_ids),
                )
            else:
                POSTGRES_PROJECTION_ERRORS_TOTAL.labels("batch").inc()
            return result

    async def _project_one(
        self,
        connection: asyncpg.Connection,
        event: PipelineEvent,
        *,
        topic: str | None = None,
        partition: int | None = None,
        offset: int | None = None,
    ) -> bool:
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
        await self._project_usage(
            connection,
            event,
            topic=topic,
            partition=partition,
            offset=offset,
        )
        return True

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
            agent = event.payload.get("agent", {})
            await connection.execute(
                """
                INSERT INTO calls (
                    call_id, status, started_at, current_stage,
                    selected_stt_provider, selected_llm_provider, selected_tts_provider,
                    selected_stt_model, selected_llm_model, selected_tts_model,
                    organization_id, agent_id, agent_snapshot
                ) VALUES (
                    $1, 'CALL_STARTED', $2, $3, $4, $5, $6, $7, $8, $9,
                    $10, $11, $12::jsonb
                )
                ON CONFLICT (call_id) DO UPDATE SET
                    status='CALL_STARTED',
                    started_at=COALESCE(calls.started_at, EXCLUDED.started_at),
                    selected_stt_provider=EXCLUDED.selected_stt_provider,
                    selected_llm_provider=EXCLUDED.selected_llm_provider,
                    selected_tts_provider=EXCLUDED.selected_tts_provider,
                    selected_stt_model=EXCLUDED.selected_stt_model,
                    selected_llm_model=EXCLUDED.selected_llm_model,
                    selected_tts_model=EXCLUDED.selected_tts_model,
                    organization_id=COALESCE(calls.organization_id, EXCLUDED.organization_id),
                    agent_id=COALESCE(calls.agent_id, EXCLUDED.agent_id),
                    agent_snapshot=CASE
                        WHEN calls.agent_snapshot = '{}'::jsonb THEN EXCLUDED.agent_snapshot
                        ELSE calls.agent_snapshot
                    END,
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
                event.payload.get("tenant_id"),
                event.payload.get("assistant_id"),
                json.dumps(agent),
            )
        elif event.event_type == EventType.CALL_ENDED:
            result = await connection.execute(
                """
                UPDATE calls SET status='CALL_COMPLETED', current_stage=$2,
                    final_summary=$3, ended_at=$4, updated_at=NOW()
                WHERE call_id=$1 AND status <> 'CALL_FAILED'
                """,
                event.call_id,
                event.stage,
                event.payload.get("final_response"),
                event.timestamp,
            )
            if result == "UPDATE 0":
                return
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
        self,
        connection: asyncpg.Connection,
        event: PipelineEvent,
        *,
        topic: str | None = None,
        partition: int | None = None,
        offset: int | None = None,
    ) -> None:
        if not str(event.event_type).startswith("usage."):
            return
        if event.event_type == EventType.USAGE_FINALIZATION_BARRIER:
            await self._project_usage_manifest(
                connection,
                event,
                topic=topic or "usage-events",
                partition=partition if partition is not None else -1,
                offset=offset if offset is not None else -1,
            )
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
            normalized_type = self._normalized_usage_type(usage_type, event.stage)
            usage_event_id = uuid5(NAMESPACE_URL, f"voicemesh:usage:{event.event_id}:{usage_type}")
            await connection.execute(
                """
                INSERT INTO call_usage_events (
                    event_id, tenant_id, assistant_id, call_id, turn_id, usage_type,
                    provider, model, quantity, unit, cost_basis_json, created_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12)
                ON CONFLICT (event_id) DO NOTHING
                """,
                usage_event_id,
                str(event.payload.get("tenant_id", "local-demo-tenant")),
                str(event.payload.get("assistant_id", "local-demo-assistant")),
                event.call_id,
                event.turn_id,
                normalized_type,
                provider,
                model,
                quantity,
                price["unit"],
                json.dumps(
                    {
                        "source_usage_type": usage_type,
                        "unit_price_usd": str(price["unit_price_usd"]),
                        "cost_usd": str(cost),
                        "pricing_version": price["pricing_version"],
                    }
                ),
                event.timestamp,
            )
            await self._update_usage_rollup(
                connection,
                event.call_id,
                str(event.payload.get("tenant_id", "local-demo-tenant")),
                str(event.payload.get("assistant_id", "local-demo-assistant")),
                normalized_type,
                quantity,
            )

        billing = await self._update_call_billing(
            connection, event.call_id, duration_seconds=None, finalized=False
        )
        await self._enqueue_billing_event(connection, event, billing)

    async def _project_usage_manifest(
        self,
        connection: asyncpg.Connection,
        event: PipelineEvent,
        *,
        topic: str,
        partition: int,
        offset: int,
    ) -> None:
        tenant_id = str(event.payload.get("tenant_id", "local-demo-tenant"))
        assistant_id = str(event.payload.get("assistant_id", "local-demo-assistant"))
        expected_turns = event.payload.get("expected_turns", [])
        await connection.execute(
            """
            INSERT INTO call_usage_manifests (
                call_id, tenant_id, assistant_id, event_id, barrier_topic,
                barrier_partition, barrier_offset, expected_turns, trace_id, created_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10)
            ON CONFLICT (call_id) DO UPDATE SET
                event_id=EXCLUDED.event_id,
                barrier_topic=EXCLUDED.barrier_topic,
                barrier_partition=EXCLUDED.barrier_partition,
                barrier_offset=EXCLUDED.barrier_offset,
                expected_turns=EXCLUDED.expected_turns,
                trace_id=COALESCE(EXCLUDED.trace_id, call_usage_manifests.trace_id),
                updated_at=NOW()
            """,
            event.call_id,
            tenant_id,
            assistant_id,
            event.event_id,
            topic,
            partition,
            offset,
            json.dumps(expected_turns),
            event.trace_id,
            event.timestamp,
        )
        for turn in expected_turns:
            turn_id = str(turn.get("turn_id", ""))
            if not turn_id:
                continue
            for usage_type in turn.get("expected_usage", []):
                normalized = str(usage_type)
                await connection.execute(
                    """
                    INSERT INTO call_usage_expectations (
                        call_id, tenant_id, assistant_id, turn_id, usage_type, source_stage
                    ) VALUES ($1,$2,$3,$4,$5,$6)
                    ON CONFLICT (call_id, turn_id, usage_type) DO NOTHING
                    """,
                    event.call_id,
                    tenant_id,
                    assistant_id,
                    turn_id,
                    normalized,
                    self._stage_for_usage_type(normalized),
                )

    def _normalized_usage_type(self, usage_type: str, stage: str) -> str:
        mapping = {
            "audio_minute": "stt_audio_seconds" if stage == "stt" else "call_duration_seconds",
            "input_token": "llm_input_tokens",
            "cached_input_token": "llm_input_tokens",
            "output_token": "llm_output_tokens",
            "input_text_token": "tts_characters",
            "output_audio_token": "tts_audio_seconds",
        }
        return mapping.get(usage_type, usage_type)

    def _stage_for_usage_type(self, usage_type: str) -> str:
        if usage_type.startswith("stt_"):
            return "stt"
        if usage_type.startswith("llm_"):
            return "llm"
        if usage_type.startswith("tts_"):
            return "tts"
        if usage_type.startswith("telephony_"):
            return "telephony"
        return "usage"

    async def _update_usage_rollup(
        self,
        connection: asyncpg.Connection,
        call_id: str,
        tenant_id: str,
        assistant_id: str,
        usage_type: str,
        quantity: Decimal,
    ) -> None:
        columns = {
            "stt_audio_seconds",
            "llm_input_tokens",
            "llm_output_tokens",
            "tts_characters",
            "tts_audio_seconds",
            "telephony_seconds",
        }
        if usage_type not in columns:
            return
        await connection.execute(
            f"""
            INSERT INTO call_usage_rollups (call_id, tenant_id, assistant_id, {usage_type})
            VALUES ($1,$2,$3,$4)
            ON CONFLICT (call_id) DO UPDATE SET
                {usage_type}=call_usage_rollups.{usage_type} + EXCLUDED.{usage_type},
                updated_at=NOW()
            """,
            call_id,
            tenant_id,
            assistant_id,
            quantity,
        )

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
