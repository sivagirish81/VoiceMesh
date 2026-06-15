from typing import Any, cast

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from apps.api.db.repository import PostgresRepository
from apps.api.events.kafka_producer import KafkaEventProducer
from apps.api.events.schemas import PipelineEvent
from apps.api.failure_injection.injector import FailureInjector

router = APIRouter(prefix="/demo", tags=["demo"])


class FailureInjectionRequest(BaseModel):
    enabled: bool | None = None
    stt_delay_ms: int | None = Field(default=None, ge=0, le=120_000)
    llm_delay_ms: int | None = Field(default=None, ge=0, le=120_000)
    tts_delay_ms: int | None = Field(default=None, ge=0, le=120_000)
    provider_failure: bool | None = None
    postgres_failure: bool | None = None
    stage_timeout: bool | None = None


@router.get("/failure-injection")
async def get_failure_injection(request: Request) -> dict[str, object]:
    injector = cast(FailureInjector, request.app.state.failure_injector)
    return injector.snapshot()


@router.post("/failure-injection")
async def set_failure_injection(
    body: FailureInjectionRequest, request: Request
) -> dict[str, object]:
    values = body.model_dump(exclude_none=True)
    injector = cast(FailureInjector, request.app.state.failure_injector)
    return await injector.update(values)


@router.post("/replay-duplicate-events/{call_id}")
async def replay_duplicate_events(call_id: str, request: Request) -> dict[str, Any]:
    repository = cast(PostgresRepository, request.app.state.repository)
    producer = cast(KafkaEventProducer, request.app.state.producer)
    events = await repository.get_events(call_id)
    if not events:
        raise HTTPException(status_code=404, detail="No persisted events for call")
    source = events[-1]
    replay = PipelineEvent(
        event_id=source["event_id"],
        call_id=source["call_id"],
        turn_id=source["turn_id"],
        event_type=source["event_type"],
        stage=source["stage"],
        timestamp=source["created_at"],
        sequence_number=source["sequence_number"],
        idempotency_key=source["idempotency_key"],
        payload=source["payload"],
        trace_id=source["trace_id"],
    )
    await producer.publish(replay)
    return {
        "call_id": call_id,
        "duplicate_replay_queued": True,
        "idempotency_key": replay.idempotency_key,
    }


@router.post("/reset")
async def reset_demo(request: Request) -> dict[str, bool]:
    injector = cast(FailureInjector, request.app.state.failure_injector)
    repository = cast(PostgresRepository, request.app.state.repository)
    await injector.reset()
    await repository.reset_demo()
    return {"reset": True}
