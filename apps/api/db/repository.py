import asyncio
import json
import logging
import re
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, TypeVar, cast

import asyncpg
from opentelemetry import trace

from apps.api.events.schemas import PipelineEvent, topic_for_event
from apps.api.failure_injection.injector import FailureInjector
from apps.api.security import hash_password, verify_password
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
        self,
        call_id: str,
        stt_provider: str,
        llm_provider: str,
        tts_provider: str,
        *,
        organization_id: str | None = None,
        agent_id: str | None = None,
        agent_snapshot: dict[str, Any] | None = None,
        stt_model: str | None = None,
        llm_model: str | None = None,
        tts_model: str | None = None,
    ) -> bool:
        async def operation() -> str:
            return cast(
                str,
                await self._require_pool().execute(
                    """
                    INSERT INTO calls (
                        call_id, status, started_at, selected_stt_provider,
                        selected_llm_provider, selected_tts_provider,
                        organization_id, agent_id, agent_snapshot,
                        selected_stt_model, selected_llm_model, selected_tts_model
                    ) VALUES (
                        $1, 'CALL_STARTED', NOW(), $2, $3, $4,
                        $5, $6, $7::jsonb, $8, $9, $10
                    )
                    ON CONFLICT (call_id) DO NOTHING
                    """,
                    call_id,
                    stt_provider,
                    llm_provider,
                    tts_provider,
                    organization_id,
                    agent_id,
                    json.dumps(agent_snapshot or {}),
                    stt_model,
                    llm_model,
                    tts_model,
                ),
            )

        result = await self._retry(operation)
        return result == "INSERT 0 1"

    async def ensure_platform_seed(self, settings: Any) -> dict[str, Any]:
        pool = self._require_pool()
        if not settings.voicemesh_admin_email or not settings.voicemesh_admin_password:
            raise RuntimeError(
                "VOICEMESH_ADMIN_EMAIL and VOICEMESH_ADMIN_PASSWORD are required "
                "to seed the local workspace. Set them in .env."
            )
        default_prompt = (
            "You are a concise VoiceMesh voice agent. Help the caller clearly, "
            "ask one question at a time, and keep responses easy to speak aloud."
        )
        async with pool.acquire() as connection, connection.transaction():
            org = await connection.fetchrow(
                """
                INSERT INTO organizations (name, slug)
                VALUES ($1, 'voicemesh-local')
                ON CONFLICT (slug) DO UPDATE
                    SET name = EXCLUDED.name, updated_at = NOW()
                RETURNING *
                """,
                settings.voicemesh_default_org_name,
            )
            assert org is not None
            agent = await connection.fetchrow(
                """
                INSERT INTO voice_agents (
                    organization_id, name, description, status, system_prompt,
                    context_prompt, first_message, stt_provider, stt_model,
                    llm_provider, llm_model, tts_provider, tts_model, tts_voice
                )
                VALUES (
                    $1, 'Default Voice Agent',
                    'Local seeded agent used for compatibility and quick testing.',
                    'active', $2, '',
                    'Hi, this is VoiceMesh. How can I help?',
                    $3, $4, $5, $6, $7, $8, $9
                )
                ON CONFLICT (organization_id, name) DO UPDATE SET
                    stt_provider = EXCLUDED.stt_provider,
                    stt_model = EXCLUDED.stt_model,
                    llm_provider = EXCLUDED.llm_provider,
                    llm_model = EXCLUDED.llm_model,
                    tts_provider = EXCLUDED.tts_provider,
                    tts_model = EXCLUDED.tts_model,
                    tts_voice = EXCLUDED.tts_voice,
                    updated_at = NOW()
                RETURNING *
                """,
                org["id"],
                default_prompt,
                settings.stt_provider,
                settings.openai_stt_model,
                settings.llm_provider,
                settings.openai_llm_model,
                settings.tts_provider,
                settings.openai_tts_model,
                settings.openai_tts_voice,
            )
            assert agent is not None
            existing_user = await connection.fetchrow(
                "SELECT * FROM users WHERE email = $1",
                settings.voicemesh_admin_email.lower(),
            )
            if existing_user:
                user = existing_user
            else:
                user = await connection.fetchrow(
                    """
                    INSERT INTO users (email, name, password_hash)
                    VALUES ($1, 'VoiceMesh Admin', $2)
                    RETURNING *
                    """,
                    settings.voicemesh_admin_email.lower(),
                    hash_password(settings.voicemesh_admin_password),
                )
            assert user is not None
            await connection.execute(
                """
                INSERT INTO organization_memberships (organization_id, user_id, role)
                VALUES ($1, $2, 'owner')
                ON CONFLICT (organization_id, user_id) DO UPDATE SET role='owner'
                """,
                org["id"],
                user["id"],
            )
            snapshot = self._agent_snapshot(dict(agent))
            await connection.execute(
                """
                UPDATE calls SET
                    organization_id = COALESCE(organization_id, $1),
                    agent_id = COALESCE(agent_id, $2),
                    agent_snapshot = CASE
                        WHEN agent_snapshot = '{}'::jsonb THEN $3::jsonb
                        ELSE agent_snapshot
                    END
                WHERE organization_id IS NULL
                   OR agent_id IS NULL
                   OR agent_snapshot = '{}'::jsonb
                """,
                org["id"],
                agent["id"],
                json.dumps(snapshot),
            )
            return {"organization": dict(org), "user": dict(user), "agent": dict(agent)}

    def _agent_snapshot(self, agent: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": str(agent["id"]),
            "organization_id": str(agent["organization_id"]),
            "name": agent["name"],
            "description": agent.get("description") or "",
            "status": agent.get("status") or "active",
            "system_prompt": agent["system_prompt"],
            "context_prompt": agent.get("context_prompt") or "",
            "first_message": agent.get("first_message") or "",
            "stt_provider": agent["stt_provider"],
            "stt_model": agent["stt_model"],
            "llm_provider": agent["llm_provider"],
            "llm_model": agent["llm_model"],
            "tts_provider": agent["tts_provider"],
            "tts_model": agent["tts_model"],
            "tts_voice": agent["tts_voice"],
            "tuning": agent.get("tuning") or {},
        }

    async def login_user(self, email: str, password: str) -> dict[str, Any] | None:
        row = await self._require_pool().fetchrow(
            """
            SELECT u.*, om.organization_id, om.role, o.name AS organization_name
            FROM users u
            JOIN organization_memberships om ON om.user_id = u.id
            JOIN organizations o ON o.id = om.organization_id
            WHERE lower(u.email) = lower($1)
            ORDER BY om.role = 'owner' DESC, om.created_at ASC
            LIMIT 1
            """,
            email,
        )
        if not row or not verify_password(password, row["password_hash"]):
            return None
        return dict(row)

    def _slugify_org(self, name: str) -> str:
        slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
        return slug or "workspace"

    async def register_workspace(
        self,
        *,
        email: str,
        password: str,
        name: str,
        organization_name: str,
        settings: Any,
    ) -> dict[str, Any]:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.transaction():
            existing = await connection.fetchval(
                "SELECT TRUE FROM users WHERE lower(email)=lower($1)",
                email,
            )
            if existing:
                raise ValueError("email_already_registered")
            base_slug = self._slugify_org(organization_name)
            slug = base_slug
            suffix = 2
            while await connection.fetchval(
                "SELECT TRUE FROM organizations WHERE slug=$1",
                slug,
            ):
                slug = f"{base_slug}-{suffix}"
                suffix += 1
            org = await connection.fetchrow(
                """
                INSERT INTO organizations (name, slug)
                VALUES ($1, $2)
                RETURNING *
                """,
                organization_name,
                slug,
            )
            assert org is not None
            user = await connection.fetchrow(
                """
                INSERT INTO users (email, name, password_hash)
                VALUES ($1, $2, $3)
                RETURNING *
                """,
                email.lower(),
                name,
                hash_password(password),
            )
            assert user is not None
            await connection.execute(
                """
                INSERT INTO organization_memberships (organization_id, user_id, role)
                VALUES ($1, $2, 'owner')
                """,
                org["id"],
                user["id"],
            )
            agent = await connection.fetchrow(
                """
                INSERT INTO voice_agents (
                    organization_id, name, description, status, system_prompt,
                    context_prompt, first_message, stt_provider, stt_model,
                    llm_provider, llm_model, tts_provider, tts_model, tts_voice
                )
                VALUES (
                    $1, 'Default Voice Agent',
                    'Starter agent for testing live calls in this workspace.',
                    'active',
                    'You are a concise, helpful voice agent. Speak naturally '
                    'and ask one question at a time.',
                    '',
                    'Hi, this is VoiceMesh. How can I help?',
                    $2, $3, $4, $5, $6, $7, $8
                )
                RETURNING *
                """,
                org["id"],
                settings.stt_provider,
                settings.openai_stt_model,
                settings.llm_provider,
                settings.openai_llm_model,
                settings.tts_provider,
                settings.openai_tts_model,
                settings.openai_tts_voice,
            )
            assert agent is not None
            return {
                **dict(user),
                "organization_id": org["id"],
                "organization_name": org["name"],
                "role": "owner",
                "agent_id": agent["id"],
            }

    async def create_session(
        self,
        *,
        user_id: str,
        organization_id: str,
        token_hash: str,
        ttl_hours: int,
    ) -> None:
        expires_at = datetime.now(UTC) + timedelta(hours=ttl_hours)
        await self._require_pool().execute(
            """
            INSERT INTO user_sessions (user_id, organization_id, token_hash, expires_at)
            VALUES ($1, $2, $3, $4)
            """,
            user_id,
            organization_id,
            token_hash,
            expires_at,
        )

    async def get_session_context(self, token_hash: str) -> dict[str, Any] | None:
        row = await self._require_pool().fetchrow(
            """
            SELECT
                s.id AS session_id,
                s.expires_at,
                u.id AS user_id,
                u.email,
                u.name,
                o.id AS organization_id,
                o.name AS organization_name,
                om.role
            FROM user_sessions s
            JOIN users u ON u.id = s.user_id
            JOIN organizations o ON o.id = s.organization_id
            JOIN organization_memberships om
                ON om.user_id = u.id AND om.organization_id = o.id
            WHERE s.token_hash = $1 AND s.expires_at > NOW()
            """,
            token_hash,
        )
        return dict(row) if row else None

    async def delete_session(self, token_hash: str) -> None:
        await self._require_pool().execute(
            "DELETE FROM user_sessions WHERE token_hash = $1",
            token_hash,
        )

    async def get_default_agent(self) -> dict[str, Any] | None:
        row = await self._require_pool().fetchrow(
            """
            SELECT va.*
            FROM voice_agents va
            JOIN organizations o ON o.id = va.organization_id
            WHERE o.slug = 'voicemesh-local'
            ORDER BY va.created_at ASC
            LIMIT 1
            """
        )
        return self._serialize_agent(row) if row else None

    def _serialize_agent(self, row: asyncpg.Record | dict[str, Any]) -> dict[str, Any]:
        agent = dict(row)
        agent["id"] = str(agent["id"])
        agent["organization_id"] = str(agent["organization_id"])
        return agent

    async def list_agents(self, organization_id: str) -> list[dict[str, Any]]:
        rows = await self._require_pool().fetch(
            """
            SELECT va.*,
                COUNT(c.call_id) AS recent_call_count,
                MAX(c.started_at) AS last_call_at
            FROM voice_agents va
            LEFT JOIN calls c ON c.agent_id = va.id AND c.organization_id = va.organization_id
            WHERE va.organization_id = $1
            GROUP BY va.id
            ORDER BY va.created_at DESC
            """,
            organization_id,
        )
        return [self._serialize_agent(row) for row in rows]

    async def get_agent(
        self, organization_id: str, agent_id: str
    ) -> dict[str, Any] | None:
        row = await self._require_pool().fetchrow(
            "SELECT * FROM voice_agents WHERE organization_id=$1 AND id=$2",
            organization_id,
            agent_id,
        )
        return self._serialize_agent(row) if row else None

    async def create_agent(
        self, organization_id: str, data: dict[str, Any]
    ) -> dict[str, Any]:
        row = await self._require_pool().fetchrow(
            """
            INSERT INTO voice_agents (
                organization_id, name, description, status, system_prompt,
                context_prompt, first_message, stt_provider, stt_model,
                llm_provider, llm_model, tts_provider, tts_model, tts_voice, tuning
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb)
            RETURNING *
            """,
            organization_id,
            data["name"],
            data.get("description", ""),
            data.get("status", "active"),
            data["system_prompt"],
            data.get("context_prompt", ""),
            data.get("first_message", ""),
            data.get("stt_provider", "openai"),
            data.get("stt_model", "gpt-realtime-whisper"),
            data.get("llm_provider", "openai"),
            data.get("llm_model", "gpt-4.1-mini"),
            data.get("tts_provider", "openai"),
            data.get("tts_model", "gpt-4o-mini-tts"),
            data.get("tts_voice", "alloy"),
            json.dumps(data.get("tuning") or {}),
        )
        assert row is not None
        return self._serialize_agent(row)

    async def update_agent(
        self, organization_id: str, agent_id: str, data: dict[str, Any]
    ) -> dict[str, Any] | None:
        current = await self.get_agent(organization_id, agent_id)
        if not current:
            return None
        merged = {**current, **{key: value for key, value in data.items() if value is not None}}
        row = await self._require_pool().fetchrow(
            """
            UPDATE voice_agents SET
                name=$3,
                description=$4,
                status=$5,
                system_prompt=$6,
                context_prompt=$7,
                first_message=$8,
                stt_provider=$9,
                stt_model=$10,
                llm_provider=$11,
                llm_model=$12,
                tts_provider=$13,
                tts_model=$14,
                tts_voice=$15,
                tuning=$16::jsonb,
                updated_at=NOW()
            WHERE organization_id=$1 AND id=$2
            RETURNING *
            """,
            organization_id,
            agent_id,
            merged["name"],
            merged.get("description", ""),
            merged.get("status", "active"),
            merged["system_prompt"],
            merged.get("context_prompt", ""),
            merged.get("first_message", ""),
            merged.get("stt_provider", "openai"),
            merged.get("stt_model", "gpt-realtime-whisper"),
            merged.get("llm_provider", "openai"),
            merged.get("llm_model", "gpt-4.1-mini"),
            merged.get("tts_provider", "openai"),
            merged.get("tts_model", "gpt-4o-mini-tts"),
            merged.get("tts_voice", "alloy"),
            json.dumps(merged.get("tuning") or {}),
        )
        return self._serialize_agent(row) if row else None

    async def list_agent_calls(
        self, organization_id: str, agent_id: str, limit: int = 50
    ) -> list[dict[str, Any]]:
        rows = await self._require_pool().fetch(
            """
            SELECT c.*, va.name AS agent_name
            FROM calls c
            LEFT JOIN voice_agents va ON va.id = c.agent_id
            WHERE c.organization_id=$1 AND c.agent_id=$2
            ORDER BY c.created_at DESC
            LIMIT $3
            """,
            organization_id,
            agent_id,
            limit,
        )
        return [dict(row) for row in rows]

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

    async def list_calls(
        self, limit: int = 100, organization_id: str | None = None
    ) -> list[dict[str, Any]]:
        if organization_id:
            rows = await self._require_pool().fetch(
                """
                SELECT c.*, va.name AS agent_name
                FROM calls c
                LEFT JOIN voice_agents va ON va.id = c.agent_id
                WHERE c.organization_id=$2
                ORDER BY c.created_at DESC
                LIMIT $1
                """,
                limit,
                organization_id,
            )
        else:
            rows = await self._require_pool().fetch(
                """
                SELECT c.*, va.name AS agent_name
                FROM calls c
                LEFT JOIN voice_agents va ON va.id = c.agent_id
                ORDER BY c.created_at DESC
                LIMIT $1
                """,
                limit,
            )
        return [dict(row) for row in rows]

    async def get_call(
        self, call_id: str, organization_id: str | None = None
    ) -> dict[str, Any] | None:
        if organization_id:
            row = await self._require_pool().fetchrow(
                """
                SELECT c.*, va.name AS agent_name
                FROM calls c
                LEFT JOIN voice_agents va ON va.id = c.agent_id
                WHERE c.call_id=$1 AND c.organization_id=$2
                """,
                call_id,
                organization_id,
            )
        else:
            row = await self._require_pool().fetchrow(
                """
                SELECT c.*, va.name AS agent_name
                FROM calls c
                LEFT JOIN voice_agents va ON va.id = c.agent_id
                WHERE c.call_id=$1
                """,
                call_id,
            )
        return dict(row) if row else None

    async def get_events(
        self, call_id: str, organization_id: str | None = None
    ) -> list[dict[str, Any]]:
        if organization_id:
            rows = await self._require_pool().fetch(
                """
                SELECT ce.*
                FROM call_events ce
                JOIN calls c ON c.call_id = ce.call_id
                WHERE ce.call_id=$1 AND c.organization_id=$2
                ORDER BY ce.created_at, ce.sequence_number
                """,
                call_id,
                organization_id,
            )
        else:
            rows = await self._require_pool().fetch(
                "SELECT * FROM call_events WHERE call_id=$1 ORDER BY created_at, sequence_number",
                call_id,
            )
        events = [dict(row) for row in rows]
        for event in events:
            if isinstance(event["payload"], str):
                event["payload"] = json.loads(event["payload"])
        return events

    async def get_metrics(
        self, call_id: str, organization_id: str | None = None
    ) -> list[dict[str, Any]]:
        if organization_id:
            rows = await self._require_pool().fetch(
                """
                SELECT pm.*
                FROM pipeline_metrics pm
                JOIN calls c ON c.call_id = pm.call_id
                WHERE pm.call_id=$1 AND c.organization_id=$2
                ORDER BY pm.created_at
                """,
                call_id,
                organization_id,
            )
        else:
            rows = await self._require_pool().fetch(
                "SELECT * FROM pipeline_metrics WHERE call_id=$1 ORDER BY created_at", call_id
            )
        return [dict(row) for row in rows]

    async def metrics_summary(
        self, organization_id: str | None = None
    ) -> Sequence[asyncpg.Record]:
        org_filter = "JOIN calls c ON c.call_id = pm.call_id WHERE c.organization_id=$1"
        args: tuple[str, ...] = (organization_id,) if organization_id else ()
        return cast(
            Sequence[asyncpg.Record],
            await self._require_pool().fetch(
                f"""
                SELECT stage,
                       percentile_cont(0.50) WITHIN GROUP (ORDER BY latency_ms) AS p50,
                       percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95,
                       percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_ms) AS p99,
                       COUNT(*) AS samples
                FROM pipeline_metrics pm
                {org_filter if organization_id else ""}
                GROUP BY stage
                ORDER BY stage
                """,
                *args,
            ),
        )

    async def billing_summary(self, organization_id: str | None = None) -> dict[str, Any]:
        pool = self._require_pool()
        org_join = "LEFT JOIN calls c ON c.call_id = f.call_id"
        org_where = "WHERE c.organization_id=$1" if organization_id else ""
        args: tuple[str, ...] = (organization_id,) if organization_id else ()
        totals = await pool.fetchrow(
            f"""
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
            {org_join if organization_id else ""}
            {org_where}
            """,
            *args,
        )
        live_join = "LEFT JOIN calls c ON c.call_id = cb.call_id"
        live_where = "WHERE c.organization_id=$1" if organization_id else ""
        live_totals = await pool.fetchrow(
            f"""
            SELECT
                COUNT(*) AS calls,
                COALESCE(SUM(cb.call_duration_seconds), 0) AS duration_seconds,
                COALESCE(SUM(cb.provider_cost_usd), 0) AS provider_cost_usd,
                COALESCE(SUM(cb.platform_fee_usd), 0) AS platform_fee_usd,
                COALESCE(SUM(cb.total_cost_usd), 0) AS total_cost_usd
            FROM call_billing cb
            {live_join if organization_id else ""}
            {live_where}
            """,
            *args,
        )
        usage_join = "JOIN calls c ON c.call_id = ur.call_id"
        usage_where = "WHERE c.organization_id=$1" if organization_id else ""
        stages = await pool.fetch(
            f"""
            SELECT ur.stage, ur.provider, ur.model, ur.usage_type, ur.unit,
                SUM(ur.quantity) AS quantity,
                SUM(ur.cost_usd) AS cost_usd,
                BOOL_OR(ur.estimated) AS has_estimates
            FROM usage_records ur
            {usage_join if organization_id else ""}
            {usage_where}
            GROUP BY ur.stage, ur.provider, ur.model, ur.usage_type, ur.unit
            ORDER BY ur.stage, ur.usage_type
            """,
            *args,
        )
        return {
            "totals": {
                **(dict(live_totals) if live_totals else {}),
                **(dict(totals) if totals else {}),
            },
            "usage": [dict(row) for row in stages],
        }

    async def list_billing_calls(
        self, limit: int = 100, organization_id: str | None = None
    ) -> list[dict[str, Any]]:
        org_filter = "WHERE c.organization_id=$2" if organization_id else ""
        args: tuple[Any, ...] = (limit, organization_id) if organization_id else (limit,)
        rows = await self._require_pool().fetch(
            f"""
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
                c.selected_stt_model, c.selected_llm_model, c.selected_tts_model,
                c.organization_id, c.agent_id, va.name AS agent_name
            FROM call_billing b
            FULL OUTER JOIN final_call_billing_records f USING (call_id)
            LEFT JOIN calls c USING (call_id)
            LEFT JOIN voice_agents va ON va.id = c.agent_id
            {org_filter}
            ORDER BY updated_at DESC
            LIMIT $1
            """,
            *args,
        )
        return [dict(row) for row in rows]

    async def get_call_billing(
        self, call_id: str, organization_id: str | None = None
    ) -> dict[str, Any] | None:
        pool = self._require_pool()
        org_filter = "AND c.organization_id=$2" if organization_id else ""
        args: tuple[str, ...] = (call_id, organization_id) if organization_id else (call_id,)
        billing = await pool.fetchrow(
            f"""
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
                c.selected_stt_model, c.selected_llm_model, c.selected_tts_model,
                c.organization_id, c.agent_id, va.name AS agent_name
            FROM call_billing b
            FULL OUTER JOIN final_call_billing_records f USING (call_id)
            LEFT JOIN calls c USING (call_id)
            LEFT JOIN voice_agents va ON va.id = c.agent_id
            WHERE COALESCE(f.call_id, b.call_id)=$1
            {org_filter}
            """,
            *args,
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
                billing_adjustments, final_call_billing_records,
                call_usage_expectations, call_usage_manifests,
                projection_watermarks, call_usage_rollups, call_usage_events,
                usage_records, call_billing, call_events, idempotency_keys,
                outbox_events, pipeline_metrics, calls
            """
        )
