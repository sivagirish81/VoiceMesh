import asyncio
import json
import sys
from pathlib import Path
from uuid import uuid4

import asyncpg
from temporalio.client import Client

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from apps.api.config import get_settings
from apps.api.events.kafka_producer import KafkaEventProducer
from apps.api.events.schemas import EventType, PipelineEvent


async def publish_usage(
    producer: KafkaEventProducer,
    *,
    call_id: str,
    turn_id: str,
    event_type: EventType,
    stage: str,
    provider: str,
    model: str,
    usage_type: str,
    quantity: float,
    sequence: int,
) -> PipelineEvent:
    event = PipelineEvent.create(
        call_id=call_id,
        turn_id=turn_id,
        event_type=event_type,
        stage=stage,
        sequence_number=sequence,
        payload={
            "tenant_id": "local-demo-tenant",
            "assistant_id": "billing-agent",
            "provider": provider,
            "model": model,
            "measurements": [
                {
                    "usage_type": usage_type,
                    "quantity": quantity,
                    "unit": "demo",
                    "estimated": False,
                }
            ],
        },
    )
    await producer.publish(event)
    return event


async def main() -> None:
    settings = get_settings()
    call_id = f"billing-demo-{uuid4()}"
    turn_id = "turn-1"
    producer = KafkaEventProducer(settings.kafka_bootstrap_servers)
    await producer.start()
    try:
        await producer.publish(
            PipelineEvent.create(
                call_id=call_id,
                turn_id="session",
                event_type=EventType.CALL_STARTED,
                stage="transport",
                sequence_number=1,
                payload={
                    "tenant_id": "local-demo-tenant",
                    "assistant_id": "billing-agent",
                    "providers": {"stt": "openai", "llm": "openai", "tts": "openai"},
                    "models": {
                        "stt": "gpt-realtime-whisper",
                        "llm": "gpt-4.1-mini",
                        "tts": "gpt-4o-mini-tts",
                    },
                },
            )
        )
        await publish_usage(
            producer,
            call_id=call_id,
            turn_id=turn_id,
            event_type=EventType.USAGE_STT_RECORDED,
            stage="stt",
            provider="openai",
            model="gpt-realtime-whisper",
            usage_type="audio_minute",
            quantity=3,
            sequence=2,
        )
        await publish_usage(
            producer,
            call_id=call_id,
            turn_id=turn_id,
            event_type=EventType.USAGE_LLM_RECORDED,
            stage="llm",
            provider="openai",
            model="gpt-4.1-mini",
            usage_type="input_token",
            quantity=25000,
            sequence=3,
        )
        await publish_usage(
            producer,
            call_id=call_id,
            turn_id=turn_id,
            event_type=EventType.USAGE_LLM_RECORDED,
            stage="llm",
            provider="openai",
            model="gpt-4.1-mini",
            usage_type="output_token",
            quantity=15000,
            sequence=4,
        )
        await producer.publish(
            PipelineEvent.create(
                call_id=call_id,
                turn_id="session",
                event_type=EventType.USAGE_FINALIZATION_BARRIER,
                stage="billing",
                sequence_number=5,
                payload={
                    "tenant_id": "local-demo-tenant",
                    "assistant_id": "billing-agent",
                    "pricing_version": settings.billing_pricing_version,
                    "expected_turns": [
                        {
                            "turn_id": turn_id,
                            "expected_usage": [
                                "stt_audio_seconds",
                                "llm_input_tokens",
                                "llm_output_tokens",
                                "tts_characters",
                                "tts_audio_seconds",
                            ],
                        }
                    ],
                },
            )
        )
        await producer.publish(
            PipelineEvent.create(
                call_id=call_id,
                turn_id="session",
                event_type=EventType.CALL_ENDED,
                stage="transport",
                sequence_number=6,
                payload={
                    "tenant_id": "local-demo-tenant",
                    "assistant_id": "billing-agent",
                    "duration_seconds": 240,
                    "final_response": "demo complete",
                },
            )
        )
        await asyncio.sleep(4)
        await publish_usage(
            producer,
            call_id=call_id,
            turn_id=turn_id,
            event_type=EventType.USAGE_TTS_RECORDED,
            stage="tts",
            provider="openai",
            model="gpt-4o-mini-tts",
            usage_type="input_text_token",
            quantity=60000,
            sequence=7,
        )
        await publish_usage(
            producer,
            call_id=call_id,
            turn_id=turn_id,
            event_type=EventType.USAGE_TTS_RECORDED,
            stage="tts",
            provider="openai",
            model="gpt-4o-mini-tts",
            usage_type="output_audio_token",
            quantity=4800,
            sequence=8,
        )
    finally:
        await producer.stop()

    client = await Client.connect(settings.temporal_address, namespace=settings.temporal_namespace)
    handle = client.get_workflow_handle(f"billing-{call_id}")
    result = await handle.result()

    producer = KafkaEventProducer(settings.kafka_bootstrap_servers)
    await producer.start()
    try:
        late_event = await publish_usage(
            producer,
            call_id=call_id,
            turn_id=turn_id,
            event_type=EventType.USAGE_LLM_RECORDED,
            stage="llm",
            provider="openai",
            model="gpt-4.1-mini",
            usage_type="output_token",
            quantity=250,
            sequence=9,
        )
    finally:
        await producer.stop()
    await asyncio.sleep(5)

    connection = await asyncpg.connect(settings.database_url)
    try:
        final = await connection.fetchrow(
            "SELECT * FROM final_call_billing_records WHERE call_id=$1",
            call_id,
        )
        usage = await connection.fetch(
            """
            SELECT usage_type, quantity
            FROM call_usage_events
            WHERE call_id=$1
            ORDER BY created_at
            """,
            call_id,
        )
        adjustments = await connection.fetch(
            """
            SELECT adjustment_id, previous_total_cost_cents, recomputed_total_cost_cents,
                   delta_cost_cents, reason, status
            FROM billing_adjustments
            WHERE call_id=$1
            ORDER BY created_at
            """,
            call_id,
        )
    finally:
        await connection.close()
    print(
        json.dumps(
            {
                "call_id": call_id,
                "workflow_result": result,
                "final_billing": dict(final) if final else None,
                "usage_events": [dict(row) for row in usage],
                "post_finalization_late_event_id": str(late_event.event_id),
                "billing_adjustments": [dict(row) for row in adjustments],
            },
            default=str,
            indent=2,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
