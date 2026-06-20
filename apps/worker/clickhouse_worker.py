import asyncio
import logging

from apps.api.analytics.clickhouse.consumer import ClickHouseAnalyticsConsumer
from apps.api.config import get_settings
from apps.api.telemetry.metrics import start_metrics_http_server
from apps.api.telemetry.tracing import configure_tracing


def configure_logging(level: str) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    configure_tracing("voicemesh-clickhouse-worker", settings.otel_exporter_otlp_endpoint)
    start_metrics_http_server(settings.clickhouse_worker_metrics_port)
    consumer = ClickHouseAnalyticsConsumer(settings)
    try:
        await consumer.run()
    finally:
        await consumer.close()


if __name__ == "__main__":
    asyncio.run(main())
