from fastapi import APIRouter, Request

router = APIRouter(prefix="/metrics", tags=["metrics"])


@router.get("/summary")
async def metrics_summary(request: Request) -> dict[str, object]:
    rows = await request.app.state.repository.metrics_summary()
    stages = [{key: row[key] for key in row.keys()} for row in rows]
    return {"stages": stages}

