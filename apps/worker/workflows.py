from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from typing import Any

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from apps.worker.activities import (
        emit_recovery_event,
        mark_call_completed,
        mark_call_failed,
        persist_call_state,
        select_fallback_provider,
        summarize_call,
    )


class CallState(StrEnum):
    PENDING = "PENDING"
    CALL_STARTED = "CALL_STARTED"
    IN_PROGRESS = "IN_PROGRESS"
    BACKPRESSURE_ACTIVE = "BACKPRESSURE_ACTIVE"
    PROVIDER_DEGRADED = "PROVIDER_DEGRADED"
    FALLBACK_SELECTED = "FALLBACK_SELECTED"
    CALL_FAILED = "CALL_FAILED"
    CALL_COMPLETED = "CALL_COMPLETED"
    RECOVERY_REQUIRED = "RECOVERY_REQUIRED"
    DONE = "DONE"


@dataclass
class WorkflowEvent:
    kind: str
    payload: dict[str, Any]


@workflow.defn(name="CallWorkflow")
class CallWorkflow:
    def __init__(self) -> None:
        self.state = CallState.PENDING
        self.call_id = ""
        self.events: list[WorkflowEvent] = []
        self.summary = ""
        self.done = False

    @workflow.run
    async def run(self, input_data: dict[str, Any]) -> dict[str, Any]:
        self.call_id = str(input_data["call_id"])
        timeout_seconds = int(input_data.get("timeout_seconds", 3600))
        await self._persist()
        try:
            while not self.done:
                await workflow.wait_condition(
                    lambda: bool(self.events) or self.done,
                    timeout=timedelta(seconds=timeout_seconds),
                )
                while self.events:
                    await self._apply(self.events.pop(0))
        except TimeoutError:
            self.state = CallState.RECOVERY_REQUIRED
            await self._persist()
            await workflow.execute_activity(
                emit_recovery_event,
                {"call_id": self.call_id, "reason": "call lifecycle timeout"},
                start_to_close_timeout=timedelta(seconds=15),
            )
            self.state = CallState.CALL_FAILED
            await self._persist()
            await workflow.execute_activity(
                mark_call_failed,
                {"call_id": self.call_id, "error": "workflow timeout"},
                start_to_close_timeout=timedelta(seconds=15),
            )
            self.state = CallState.DONE
            self.done = True
            await self._persist()
        return {"call_id": self.call_id, "state": self.state, "summary": self.summary}

    async def _apply(self, event: WorkflowEvent) -> None:
        if event.kind == "call_started":
            self.state = CallState.CALL_STARTED
            await self._persist()
            self.state = CallState.IN_PROGRESS
            await self._persist()
        elif event.kind == "pipeline_event":
            event_type = event.payload.get("event_type")
            self.state = (
                CallState.BACKPRESSURE_ACTIVE
                if event_type == "pipeline.corked"
                else CallState.IN_PROGRESS
            )
            await self._persist()
        elif event.kind == "provider_failed":
            self.state = CallState.PROVIDER_DEGRADED
            await self._persist()
            fallback = await workflow.execute_activity(
                select_fallback_provider,
                {"call_id": self.call_id, **event.payload},
                start_to_close_timeout=timedelta(seconds=15),
            )
            self.state = (
                CallState.FALLBACK_SELECTED
                if fallback.get("fallback_provider")
                else CallState.RECOVERY_REQUIRED
            )
            await self._persist()
        elif event.kind == "call_completed":
            self.state = CallState.CALL_COMPLETED
            await self._persist()
            self.summary = await workflow.execute_activity(
                summarize_call,
                {"call_id": self.call_id, **event.payload},
                start_to_close_timeout=timedelta(seconds=30),
            )
            await workflow.execute_activity(
                mark_call_completed,
                {"call_id": self.call_id, "summary": self.summary},
                start_to_close_timeout=timedelta(seconds=15),
            )
            self.state = CallState.DONE
            self.done = True
            await self._persist()
        elif event.kind == "call_failed":
            self.state = CallState.CALL_FAILED
            await self._persist()
            await workflow.execute_activity(
                mark_call_failed,
                {"call_id": self.call_id, **event.payload},
                start_to_close_timeout=timedelta(seconds=15),
            )
            self.state = CallState.DONE
            self.done = True
            await self._persist()

    async def _persist(self) -> None:
        await workflow.execute_activity(
            persist_call_state,
            {"call_id": self.call_id, "state": str(self.state)},
            start_to_close_timeout=timedelta(seconds=15),
            retry_policy=workflow.RetryPolicy(
                initial_interval=timedelta(milliseconds=250),
                backoff_coefficient=2,
                maximum_interval=timedelta(seconds=5),
                maximum_attempts=5,
            ),
        )

    @workflow.signal
    async def call_started(self, event: dict[str, Any]) -> None:
        self.events.append(WorkflowEvent("call_started", event))

    @workflow.signal
    async def pipeline_event(self, event: dict[str, Any]) -> None:
        self.events.append(WorkflowEvent("pipeline_event", event))

    @workflow.signal
    async def provider_failed(self, event: dict[str, Any]) -> None:
        self.events.append(WorkflowEvent("provider_failed", event))

    @workflow.signal
    async def call_completed(self, event: dict[str, Any]) -> None:
        self.events.append(WorkflowEvent("call_completed", event))

    @workflow.signal
    async def call_failed(self, event: dict[str, Any]) -> None:
        self.events.append(WorkflowEvent("call_failed", event))

    @workflow.query
    def current_state(self) -> str:
        return str(self.state)

