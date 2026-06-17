import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.responses import Response

from apps.api.config import Settings, get_settings
from apps.api.db.repository import PostgresRepository
from apps.api.events.kafka_producer import KafkaEventProducer
from apps.api.failure_injection.injector import FailureInjector
from apps.api.pipeline.stream_module import StreamModule
from apps.api.providers.provider_registry import ProviderRegistry
from apps.api.routes import billing, calls, demo, health, metrics, mock_customer, tools
from apps.api.telemetry.tracing import configure_tracing
from apps.api.temporal_client import TemporalLifecycleClient
from apps.api.websocket_transport import BrowserWebSocketTransport

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","message":"%(message)s"}',
)
logger = logging.getLogger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    runtime_settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        runtime_settings.validate_provider_credentials()
        configure_tracing("voicemesh-api", runtime_settings.otel_exporter_otlp_endpoint)
        injector = FailureInjector(runtime_settings)
        repository = PostgresRepository(
            runtime_settings.database_url,
            injector,
            runtime_settings.database_pool_min_size,
            runtime_settings.database_pool_max_size,
            runtime_settings.database_command_timeout,
        )
        producer = KafkaEventProducer(runtime_settings.kafka_bootstrap_servers)
        temporal = TemporalLifecycleClient(runtime_settings)
        try:
            await repository.connect()
            await producer.start()
            await temporal.connect()
            application.state.settings = runtime_settings
            application.state.failure_injector = injector
            application.state.repository = repository
            application.state.producer = producer
            application.state.temporal = temporal
            yield
        finally:
            await producer.stop()
            await repository.close()

    application = FastAPI(
        title="VoiceMesh",
        version="0.1.0",
        description=(
            "A Vapi-inspired reliability lab based on public voice infrastructure challenges. "
            "It does not claim to represent Vapi's internal architecture."
        ),
        lifespan=lifespan,
    )
    application.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:3000"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    application.include_router(health.router)
    application.include_router(calls.router)
    application.include_router(demo.router)
    application.include_router(metrics.router)
    application.include_router(billing.router)
    application.include_router(mock_customer.router)
    application.include_router(tools.router)

    @application.get("/")
    async def root() -> dict[str, str]:
        return {"name": "VoiceMesh", "docs": "/docs", "dashboard": "http://localhost:3000"}

    @application.get("/metrics", include_in_schema=False)
    async def prometheus_metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @application.websocket("/ws/calls/{call_id}")
    async def call_websocket(websocket: WebSocket, call_id: str) -> None:
        registry = ProviderRegistry(
            application.state.settings, application.state.failure_injector
        )
        pipeline = StreamModule(
            call_id=call_id,
            settings=application.state.settings,
            transport=BrowserWebSocketTransport(websocket),
            stt=registry.stt(),
            llm=registry.llm(),
            tts=registry.tts(),
            producer=application.state.producer,
            temporal=application.state.temporal,
        )
        await pipeline.run()

    FastAPIInstrumentor.instrument_app(application, excluded_urls="/metrics")
    return application


app = create_app()
