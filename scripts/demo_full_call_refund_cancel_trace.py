import asyncio
import json
import sys
from pathlib import Path
from uuid import uuid4

import asyncpg
import httpx
from opentelemetry import trace
from opentelemetry.propagate import inject
from temporalio.client import Client

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from apps.api.events.kafka_producer import KafkaEventProducer
from apps.api.events.schemas import EventType, PipelineEvent
from apps.api.telemetry.tracing import configure_tracing, current_trace_id, set_span_attributes

TENANT_ID = "local-demo-tenant"
ASSISTANT_ID = "refund-agent"
DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/voice_lab"
KAFKA = "localhost:9094"
TEMPORAL = "localhost:7233"
PUBLIC_API = "http://localhost:8000"
INTERNAL_API = "http://api:8000"
JAEGER = "http://localhost:16686"
TEMPORAL_UI = "http://localhost:8080"
OTEL = "http://localhost:4317"

tracer = trace.get_tracer(__name__)


def refund_tool_config() -> dict[str, object]:
    return {
        "tool_name": "refund_request",
        "mode": "DURABLE_ACTION",
        "create": {
            "method": "POST",
            "url": f"{INTERNAL_API}/mock-customer/refund-requests",
            "idempotency_key_template": "{{tool_invocation_id}}:create",
            "timeout_seconds": 10,
        },
        "cancel": {
            "method": "POST",
            "url_template": (
                f"{INTERNAL_API}/mock-customer/refund-requests/"
                "{{external_request_id}}/cancel"
            ),
            "idempotency_key_template": "{{tool_invocation_id}}:cancel",
            "timeout_seconds": 10,
        },
        "status": {
            "method": "GET",
            "url_template": (
                f"{INTERNAL_API}/mock-customer/refund-requests/"
                "{{external_request_id}}"
            ),
            "timeout_seconds": 10,
        },
        "response_mapping": {
            "external_request_id": "$.refund_request_id",
            "status": "$.status",
            "message": "$.message",
        },
        "terminal_states": ["cancelled", "cannot_cancel", "failed", "refunded", "rejected"],
        "poll_interval_seconds": 2,
        "max_runtime_seconds": 30,
    }


async def publish(
    producer: KafkaEventProducer,
    call_id: str,
    turn_id: str,
    event_type: EventType,
    stage: str,
    sequence: int,
    payload: dict[str, object],
) -> None:
    await producer.publish(
        PipelineEvent.create(
            call_id=call_id,
            turn_id=turn_id,
            event_type=event_type,
            stage=stage,
            sequence_number=sequence,
            payload=payload,
            trace_id=current_trace_id(),
        )
    )


async def publish_usage(
    producer: KafkaEventProducer,
    call_id: str,
    turn_id: str,
    event_type: EventType,
    stage: str,
    provider: str,
    model: str,
    usage_type: str,
    quantity: float,
    sequence: int,
) -> None:
    await publish(
        producer,
        call_id,
        turn_id,
        event_type,
        stage,
        sequence,
        {
            "tenant_id": TENANT_ID,
            "assistant_id": ASSISTANT_ID,
            "provider": provider,
            "model": model,
            "measurements": [
                {
                    "usage_type": usage_type,
                    "quantity": quantity,
                    "unit": "demo",
                    "estimated": stage == "tts",
                }
            ],
        },
    )


def trace_headers() -> dict[str, str]:
    headers: dict[str, str] = {}
    inject(headers)
    return headers


async def workflow_run_id(client: Client, workflow_id: str) -> str:
    description = await client.get_workflow_handle(workflow_id).describe()
    return description.run_id


async def main() -> None:
    configure_tracing("voicemesh-demo-runner", OTEL)
    call_id = f"full-call-refund-{uuid4()}"
    tool_invocation_id = f"refund-{uuid4()}"
    durable_workflow_id = f"tool-{tool_invocation_id}"
    billing_workflow_id = f"billing-{call_id}"
    producer = KafkaEventProducer(KAFKA)
    temporal = await Client.connect(TEMPORAL, namespace="default")

    await producer.start()
    try:
        with tracer.start_as_current_span("demo.full_call_refund_cancel") as span:
            set_span_attributes(
                span,
                call_id=call_id,
                tenant_id=TENANT_ID,
                assistant_id=ASSISTANT_ID,
                tool_invocation_id=tool_invocation_id,
                durable_workflow_id=durable_workflow_id,
                billing_workflow_id=billing_workflow_id,
            )
            trace_id = current_trace_id()
            await publish(
                producer,
                call_id,
                "session",
                EventType.CALL_STARTED,
                "transport",
                1,
                {
                    "tenant_id": TENANT_ID,
                    "assistant_id": ASSISTANT_ID,
                    "providers": {"stt": "openai", "llm": "openai", "tts": "openai"},
                    "models": {
                        "stt": "gpt-realtime-whisper",
                        "llm": "gpt-4.1-mini",
                        "tts": "gpt-4o-mini-tts",
                    },
                    "demo_note": (
                        "Synthetic full-call control-plane demo. The media hot path "
                        "is not routed through Temporal."
                    ),
                },
            )
            await publish(
                producer,
                call_id,
                "turn-refund-request",
                EventType.STT_FINAL_TRANSCRIPT,
                "stt",
                2,
                {
                    "tenant_id": TENANT_ID,
                    "assistant_id": ASSISTANT_ID,
                    "transcript": "I want a refund for my last order.",
                },
            )
            await publish(
                producer,
                call_id,
                "turn-refund-request",
                EventType.LLM_FINAL_RESPONSE,
                "llm",
                3,
                {
                    "tenant_id": TENANT_ID,
                    "assistant_id": ASSISTANT_ID,
                    "response": "I can start a refund request for you.",
                    "tool_name": "refund_request",
                },
            )
            async with httpx.AsyncClient(timeout=20) as client:
                with tracer.start_as_current_span("demo.tool_execute_refund"):
                    accepted = await client.post(
                        f"{PUBLIC_API}/tools/execute",
                        headers=trace_headers(),
                        json={
                            "tenant_id": TENANT_ID,
                            "assistant_id": ASSISTANT_ID,
                            "call_id": call_id,
                            "turn_id": "turn-refund-request",
                            "tool_invocation_id": tool_invocation_id,
                            "tool_name": "refund_request",
                            "mode": "DURABLE_ACTION",
                            "arguments": {
                                "refund_request_id": "rr_full_traced",
                                "delay_seconds": 5,
                                "amount_cents": 1200,
                            },
                            "config": refund_tool_config(),
                        },
                    )
                    accepted.raise_for_status()

                await asyncio.sleep(1)
                await publish(
                    producer,
                    call_id,
                    "turn-cancel-request",
                    EventType.STT_FINAL_TRANSCRIPT,
                    "stt",
                    4,
                    {
                        "tenant_id": TENANT_ID,
                        "assistant_id": ASSISTANT_ID,
                        "transcript": "Actually, cancel that refund request.",
                    },
                )
                with tracer.start_as_current_span("demo.tool_cancel_refund"):
                    cancelled = await client.post(
                        f"{PUBLIC_API}/tools/durable-actions/{tool_invocation_id}/cancel",
                        headers=trace_headers(),
                        params={"reason": "customer_cancelled_next_turn"},
                    )
                    cancelled.raise_for_status()

            durable_result = await temporal.get_workflow_handle(durable_workflow_id).result()

            await publish_usage(
                producer,
                call_id,
                "turn-refund-request",
                EventType.USAGE_STT_RECORDED,
                "stt",
                "openai",
                "gpt-realtime-whisper",
                "audio_minute",
                3,
                5,
            )
            await publish_usage(
                producer,
                call_id,
                "turn-refund-request",
                EventType.USAGE_LLM_RECORDED,
                "llm",
                "openai",
                "gpt-4.1-mini",
                "input_token",
                25000,
                6,
            )
            await publish_usage(
                producer,
                call_id,
                "turn-refund-request",
                EventType.USAGE_LLM_RECORDED,
                "llm",
                "openai",
                "gpt-4.1-mini",
                "output_token",
                15000,
                7,
            )
            await publish(
                producer,
                call_id,
                "session",
                EventType.CALL_ENDED,
                "transport",
                8,
                {
                    "tenant_id": TENANT_ID,
                    "assistant_id": ASSISTANT_ID,
                    "duration_seconds": 240,
                    "final_response": "The refund request was cancelled before completion.",
                },
            )
            await asyncio.sleep(3)
            billing_handle = temporal.get_workflow_handle(billing_workflow_id)
            billing_waiting = await billing_handle.query("GetBillingState")
            await publish_usage(
                producer,
                call_id,
                "turn-refund-request",
                EventType.USAGE_TTS_RECORDED,
                "tts",
                "openai",
                "gpt-4o-mini-tts",
                "input_text_token",
                60000,
                9,
            )
            billing_result = await billing_handle.result()
    finally:
        await producer.stop()

    connection = await asyncpg.connect(DATABASE_URL)
    try:
        final_billing = await connection.fetchrow(
            """
            SELECT call_id, status, billable_seconds, platform_cost_cents,
                   stt_cost_cents, llm_cost_cents, tts_cost_cents, total_cost_cents
            FROM final_call_billing_records WHERE call_id=$1
            """,
            call_id,
        )
    finally:
        await connection.close()

    durable_run_id = await workflow_run_id(temporal, durable_workflow_id)
    billing_run_id = await workflow_run_id(temporal, billing_workflow_id)
    print(
        json.dumps(
            {
                "call_id": call_id,
                "trace_id": trace_id,
                "jaeger_trace_url": f"{JAEGER}/trace/{trace_id}",
                "jaeger_search_url": (
                    f"{JAEGER}/search?lookback=1h&service=voicemesh-demo-runner"
                ),
                "durable_action_workflow_id": durable_workflow_id,
                "durable_action_run_id": durable_run_id,
                "durable_action_temporal_url": (
                    f"{TEMPORAL_UI}/namespaces/default/workflows/"
                    f"{durable_workflow_id}/{durable_run_id}/history"
                ),
                "billing_workflow_id": billing_workflow_id,
                "billing_run_id": billing_run_id,
                "billing_temporal_url": (
                    f"{TEMPORAL_UI}/namespaces/default/workflows/"
                    f"{billing_workflow_id}/{billing_run_id}/history"
                ),
                "call_dashboard_url": f"http://localhost:3000/calls/{call_id}",
                "billing_dashboard_url": "http://localhost:3000/billing",
                "tool_execute_response": accepted.json(),
                "cancel_response": cancelled.json(),
                "durable_result": durable_result,
                "billing_state_before_late_tts": billing_waiting,
                "billing_result": billing_result,
                "final_billing": dict(final_billing) if final_billing else None,
            },
            indent=2,
            default=str,
        )
    )
    await asyncio.sleep(2)


if __name__ == "__main__":
    asyncio.run(main())
