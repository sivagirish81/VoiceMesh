from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from apps.api.routes.health import router


class HealthyRepository:
    async def health(self) -> bool:
        return True


class HealthyTemporal:
    async def health(self) -> bool:
        return True


def test_health_endpoints_work() -> None:
    app = FastAPI()
    app.state.repository = HealthyRepository()
    app.state.temporal = HealthyTemporal()
    app.state.producer = SimpleNamespace(started=True)
    app.include_router(router)
    client = TestClient(app)
    assert client.get("/health").json()["status"] == "ok"
    assert client.get("/health/kafka").json()["healthy"] is True
    assert client.get("/health/postgres").json()["healthy"] is True
    assert client.get("/health/temporal").json()["healthy"] is True

