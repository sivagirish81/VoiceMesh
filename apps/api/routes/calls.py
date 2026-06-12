from typing import cast

from fastapi import APIRouter, HTTPException, Request

from apps.api.db.repository import PostgresRepository

router = APIRouter(prefix="/calls", tags=["calls"])


@router.get("")
async def list_calls(request: Request) -> list[dict[str, object]]:
    repository = cast(PostgresRepository, request.app.state.repository)
    return await repository.list_calls()


@router.get("/{call_id}")
async def get_call(call_id: str, request: Request) -> dict[str, object]:
    repository = cast(PostgresRepository, request.app.state.repository)
    call = await repository.get_call(call_id)
    if not call:
        raise HTTPException(status_code=404, detail="Call not found")
    return call


@router.get("/{call_id}/events")
async def get_call_events(call_id: str, request: Request) -> list[dict[str, object]]:
    repository = cast(PostgresRepository, request.app.state.repository)
    return await repository.get_events(call_id)


@router.get("/{call_id}/metrics")
async def get_call_metrics(call_id: str, request: Request) -> list[dict[str, object]]:
    repository = cast(PostgresRepository, request.app.state.repository)
    return await repository.get_metrics(call_id)
