from typing import Annotated

from fastapi import APIRouter, Depends, Request

from apps.api.auth import AuthContext, get_current_context

router = APIRouter(prefix="/metrics", tags=["metrics"])


@router.get("/summary")
async def metrics_summary(
    request: Request,
    context: Annotated[AuthContext, Depends(get_current_context)],
) -> dict[str, object]:
    rows = await request.app.state.repository.metrics_summary(context.organization_id)
    stages = [{key: row[key] for key in row.keys()} for row in rows]
    return {"stages": stages}
