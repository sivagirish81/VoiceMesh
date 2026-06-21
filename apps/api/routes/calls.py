from typing import Annotated, cast

from fastapi import APIRouter, Depends, HTTPException, Request

from apps.api.auth import AuthContext, get_current_context
from apps.api.db.repository import PostgresRepository

router = APIRouter(prefix="/calls", tags=["calls"])


@router.get("")
async def list_calls(
    request: Request,
    context: Annotated[AuthContext, Depends(get_current_context)],
) -> list[dict[str, object]]:
    repository = cast(PostgresRepository, request.app.state.repository)
    return await repository.list_calls(organization_id=context.organization_id)


@router.get("/{call_id}")
async def get_call(
    call_id: str,
    request: Request,
    context: Annotated[AuthContext, Depends(get_current_context)],
) -> dict[str, object]:
    repository = cast(PostgresRepository, request.app.state.repository)
    call = await repository.get_call(call_id, context.organization_id)
    if not call:
        raise HTTPException(status_code=404, detail="Call not found")
    return call


@router.get("/{call_id}/events")
async def get_call_events(
    call_id: str,
    request: Request,
    context: Annotated[AuthContext, Depends(get_current_context)],
) -> list[dict[str, object]]:
    repository = cast(PostgresRepository, request.app.state.repository)
    return await repository.get_events(call_id, context.organization_id)


@router.get("/{call_id}/metrics")
async def get_call_metrics(
    call_id: str,
    request: Request,
    context: Annotated[AuthContext, Depends(get_current_context)],
) -> list[dict[str, object]]:
    repository = cast(PostgresRepository, request.app.state.repository)
    return await repository.get_metrics(call_id, context.organization_id)
