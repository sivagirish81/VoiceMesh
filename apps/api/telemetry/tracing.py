from collections.abc import Iterable
from typing import Any

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.propagate import extract, inject
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


def configure_tracing(service_name: str, endpoint: str) -> None:
    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True))
    )
    trace.set_tracer_provider(provider)


def current_trace_id() -> str | None:
    context = trace.get_current_span().get_span_context()
    return format(context.trace_id, "032x") if context.is_valid else None


def set_span_attributes(span: trace.Span, **attributes: Any) -> None:
    for key, value in attributes.items():
        if value is None:
            continue
        if isinstance(value, str | bool | int | float):
            span.set_attribute(key, value)
        else:
            span.set_attribute(key, str(value))


def kafka_headers_from_current_context() -> list[tuple[str, bytes]]:
    carrier: dict[str, str] = {}
    inject(carrier)
    return [(key, value.encode("utf-8")) for key, value in carrier.items()]


def context_from_kafka_headers(headers: Iterable[tuple[str, bytes]] | None) -> Any:
    carrier = {
        key: value.decode("utf-8")
        for key, value in headers or []
        if isinstance(value, bytes)
    }
    return extract(carrier)


def inject_trace_context(payload: dict[str, Any]) -> dict[str, Any]:
    carrier: dict[str, str] = {}
    inject(carrier)
    if carrier:
        payload = {**payload, "_trace": carrier}
    return payload


def context_from_payload(payload: dict[str, Any]) -> Any:
    trace_payload = payload.get("_trace")
    return extract(trace_payload if isinstance(trace_payload, dict) else {})
