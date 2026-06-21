from typing import Annotated, Any, cast

from fastapi import APIRouter, Depends, HTTPException, Request

from apps.api.auth import AuthContext, get_current_context
from apps.api.db.repository import PostgresRepository

router = APIRouter(prefix="/billing", tags=["billing"])


@router.get("/summary")
async def billing_summary(
    request: Request,
    context: Annotated[AuthContext, Depends(get_current_context)],
) -> dict[str, Any]:
    repository = cast(PostgresRepository, request.app.state.repository)
    return await repository.billing_summary(context.organization_id)


@router.get("/calls")
async def billing_calls(
    request: Request,
    context: Annotated[AuthContext, Depends(get_current_context)],
) -> list[dict[str, Any]]:
    repository = cast(PostgresRepository, request.app.state.repository)
    return await repository.list_billing_calls(organization_id=context.organization_id)


@router.get("/calls/{call_id}")
async def call_billing(
    call_id: str,
    request: Request,
    context: Annotated[AuthContext, Depends(get_current_context)],
) -> dict[str, Any]:
    repository = cast(PostgresRepository, request.app.state.repository)
    result = await repository.get_call_billing(call_id, context.organization_id)
    if not result:
        raise HTTPException(status_code=404, detail="Billing record not found")
    return result
