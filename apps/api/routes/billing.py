from typing import Any, cast

from fastapi import APIRouter, HTTPException, Request

from apps.api.db.repository import PostgresRepository

router = APIRouter(prefix="/billing", tags=["billing"])


@router.get("/summary")
async def billing_summary(request: Request) -> dict[str, Any]:
    repository = cast(PostgresRepository, request.app.state.repository)
    return await repository.billing_summary()


@router.get("/calls")
async def billing_calls(request: Request) -> list[dict[str, Any]]:
    repository = cast(PostgresRepository, request.app.state.repository)
    return await repository.list_billing_calls()


@router.get("/calls/{call_id}")
async def call_billing(call_id: str, request: Request) -> dict[str, Any]:
    repository = cast(PostgresRepository, request.app.state.repository)
    result = await repository.get_call_billing(call_id)
    if not result:
        raise HTTPException(status_code=404, detail="Billing record not found")
    return result
