import asyncio
from uuid import uuid4

from apps.api.analytics.clickhouse.client import ClickHouseCloudClient
from apps.api.analytics.clickhouse.normalizer import normalize_pipeline_event
from apps.api.analytics.clickhouse.writer import ClickHouseBatchWriter
from apps.api.config import get_settings
from apps.api.events.schemas import EventType, PipelineEvent
from scripts.clickhouse_cloud_bootstrap import BOOTSTRAP_SQL, _statements


async def main() -> None:
    settings = get_settings()
    if not settings.clickhouse_enabled:
        raise SystemExit(
            "CLICKHOUSE_ENABLED=false. Add ClickHouse Cloud credentials to .env first."
        )

    bootstrap_client = ClickHouseCloudClient(settings, database="default")
    client: ClickHouseCloudClient | None = None
    run_id = f"demo-{uuid4().hex[:10]}"
    tenant_id = f"clickhouse-{run_id}"
    assistant_id = "analytics-demo-assistant"
    try:
        sql = await asyncio.to_thread(BOOTSTRAP_SQL.read_text)
        for statement in _statements(sql):
            await bootstrap_client.command(statement)
        await bootstrap_client.close()
        client = ClickHouseCloudClient(settings)
        writer = ClickHouseBatchWriter(client, max_rows=settings.clickhouse_batch_max_rows)
        for event in _demo_events(tenant_id, assistant_id):
            await writer.add(normalize_pipeline_event(event))
        await writer.flush()
        await _print_results(client, tenant_id)
    finally:
        await bootstrap_client.close()
        if client is not None:
            await client.close()


def _demo_events(tenant_id: str, assistant_id: str) -> list[PipelineEvent]:
    events: list[PipelineEvent] = []

    def add(
        call_id: str,
        seq: int,
        event_type: EventType,
        *,
        stage: str,
        payload: dict[str, object] | None = None,
        turn_id: str = "turn-1",
    ) -> None:
        body = {"tenant_id": tenant_id, "assistant_id": assistant_id, **(payload or {})}
        events.append(
            PipelineEvent.create(
                call_id=call_id,
                turn_id=turn_id,
                event_type=event_type,
                stage=stage,
                sequence_number=seq,
                payload=body,
            )
        )

    add("ch-demo-healthy", 1, EventType.CALL_STARTED, stage="call")
    add(
        "ch-demo-healthy",
        2,
        EventType.STT_FINAL_TRANSCRIPT,
        stage="stt",
        payload={"provider": "openai", "model": "gpt-realtime-whisper", "latency_ms": 640},
    )
    add(
        "ch-demo-healthy",
        3,
        EventType.LLM_FIRST_TOKEN,
        stage="llm",
        payload={"provider": "openai", "model": "gpt-4.1-mini", "latency_ms": 310},
    )
    add(
        "ch-demo-healthy",
        4,
        EventType.TTS_FIRST_AUDIO,
        stage="tts",
        payload={"provider": "openai", "model": "gpt-4o-mini-tts", "latency_ms": 420},
    )
    add(
        "ch-demo-healthy",
        5,
        EventType.CALL_ENDED,
        stage="call",
        payload={"duration_seconds": 22, "status": "completed"},
    )
    add("ch-demo-healthy", 6, EventType.BILLING_FINALIZED, stage="billing")

    add("ch-demo-slow-tts", 1, EventType.CALL_STARTED, stage="call")
    add(
        "ch-demo-slow-tts",
        2,
        EventType.LLM_FIRST_TOKEN,
        stage="llm",
        payload={"provider": "openai", "model": "gpt-4.1-mini", "latency_ms": 260},
    )
    add(
        "ch-demo-slow-tts",
        3,
        EventType.PIPELINE_CORKED,
        stage="llm_to_tts",
        payload={"stage": "llm_to_tts", "reason_code": "queue_high_watermark"},
    )
    add(
        "ch-demo-slow-tts",
        4,
        EventType.TTS_FIRST_AUDIO,
        stage="tts",
        payload={"provider": "openai", "model": "gpt-4o-mini-tts", "latency_ms": 1800},
    )
    add(
        "ch-demo-slow-tts",
        5,
        EventType.PIPELINE_UNCORKED,
        stage="llm_to_tts",
        payload={"stage": "llm_to_tts", "duration_ms": 1250},
    )
    add("ch-demo-slow-tts", 6, EventType.CALL_ENDED, stage="call", payload={"status": "completed"})

    add("ch-demo-interaction", 1, EventType.CALL_STARTED, stage="call")
    add(
        "ch-demo-interaction",
        2,
        EventType.VAD_NOISE_TURN_IGNORED,
        stage="vad",
        payload={"reason_code": "noise_spike"},
    )
    add("ch-demo-interaction", 3, EventType.USER_BARGE_IN_CANDIDATE, stage="barge_in")
    add(
        "ch-demo-interaction",
        4,
        EventType.USER_BARGE_IN_REJECTED,
        stage="barge_in",
        payload={"reason_code": "noise_spike"},
    )
    add("ch-demo-interaction", 5, EventType.USER_BARGE_IN_CANDIDATE, stage="barge_in")
    add("ch-demo-interaction", 6, EventType.USER_BARGE_IN_CONFIRMED, stage="barge_in")
    add(
        "ch-demo-interaction",
        7,
        EventType.PIPELINE_RESPONSE_CANCELLED,
        stage="transport",
        payload={"reason_code": "barge_in_confirmed"},
    )
    add(
        "ch-demo-interaction",
        8,
        EventType.PIPELINE_STALE_CHUNK_DROPPED,
        stage="transport",
        payload={"reason_code": "cancelled_response"},
    )
    add(
        "ch-demo-interaction",
        9,
        EventType.CALL_ENDED,
        stage="call",
        payload={"status": "completed"},
    )

    add("ch-demo-provider-failure", 1, EventType.CALL_STARTED, stage="call")
    add(
        "ch-demo-provider-failure",
        2,
        EventType.PROVIDER_FAILED,
        stage="tts",
        payload={"provider": "openai", "model": "gpt-4o-mini-tts", "reason_code": "timeout"},
    )
    add(
        "ch-demo-provider-failure",
        3,
        EventType.CALL_FAILED,
        stage="call",
        payload={"status": "failed", "reason_code": "provider_timeout"},
    )
    return events


async def _print_results(client: ClickHouseCloudClient, tenant_id: str) -> None:
    results = await client.query_rows(
        f"""
        SELECT
          count() AS rows_inserted,
          uniqExact(call_id) AS unique_calls,
          uniqExactIf(call_id, event_type = 'call.ended') AS completed_calls,
          uniqExactIf(call_id, event_type = 'call.failed') AS failed_calls,
          quantileExactIf(0.95)(
            latency_ms,
            event_type = 'llm.first_token' AND latency_ms IS NOT NULL
          ) AS llm_ttft_p95,
          quantileExactIf(0.95)(
            latency_ms,
            event_type = 'tts.first_audio' AND latency_ms IS NOT NULL
          ) AS tts_first_audio_p95,
          uniqExactIf(call_id, event_type = 'pipeline.corked') AS calls_affected_by_corking,
          countIf(event_type = 'user.barge_in_candidate') AS barge_in_candidates,
          countIf(event_type = 'user.barge_in_confirmed') AS barge_ins_confirmed,
          countIf(event_type = 'user.barge_in_rejected') AS barge_ins_rejected,
          countIf(event_type = 'vad.noise_turn_ignored') AS noise_like_turns_ignored,
          countIf(event_type = 'provider.failed') AS provider_failures
        FROM voice_events
        WHERE tenant_id = '{tenant_id}'
        """
    )
    row = results[0]
    print("ClickHouse Cloud connection: OK")
    print(f"Rows inserted: {row[0]}")
    print(f"Unique calls: {row[1]}")
    print(f"Completed calls: {row[2]}")
    print(f"Failed calls: {row[3]}")
    print(f"LLM TTFT p95: {row[4]}")
    print(f"TTS first-audio p95: {row[5]}")
    print(f"Calls affected by corking: {row[6]}")
    print(f"Barge-in candidates: {row[7]}")
    print(f"Barge-ins confirmed: {row[8]}")
    print(f"Barge-ins rejected: {row[9]}")
    print(f"Noise-like turns ignored: {row[10]}")
    print(f"Provider failures: {row[11]}")


if __name__ == "__main__":
    asyncio.run(main())
