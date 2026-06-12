from fastapi import APIRouter, Request

router = APIRouter(prefix="/health", tags=["health"])


@router.get("")
async def health(request: Request) -> dict[str, object]:
    postgres = await request.app.state.repository.health()
    temporal = await request.app.state.temporal.health()
    kafka = request.app.state.producer.started
    return {
        "status": "ok" if postgres and temporal and kafka else "degraded",
        "postgres": postgres,
        "kafka": kafka,
        "temporal": temporal,
    }


@router.get("/postgres")
async def postgres_health(request: Request) -> dict[str, object]:
    healthy = await request.app.state.repository.health()
    return {"service": "postgres", "healthy": healthy}


@router.get("/kafka")
async def kafka_health(request: Request) -> dict[str, object]:
    return {"service": "kafka", "healthy": request.app.state.producer.started}


@router.get("/temporal")
async def temporal_health(request: Request) -> dict[str, object]:
    return {"service": "temporal", "healthy": await request.app.state.temporal.health()}

