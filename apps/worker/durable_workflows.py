from datetime import timedelta
from typing import Any, cast

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from apps.api.tools.models import (
        BillingFinalizationInput,
        BillingStatus,
        BillingWorkflowState,
        DurableActionInput,
        DurableActionState,
        DurableActionStatus,
        WebhookDeliveryInput,
        WebhookDeliveryState,
    )
    from apps.worker.activities import (
        cancel_external_action,
        create_billing_adjustment,
        create_external_action,
        deliver_webhook,
        emit_tool_event,
        emit_workflow_event,
        finalize_call_billing,
        get_external_action_status,
        load_billing_readiness,
        mark_call_completed,
        persist_tool_state,
        persist_webhook_delivery,
        summarize_call,
    )


def _validated_payload_with_trace(parsed: Any, original: dict[str, Any]) -> dict[str, Any]:
    payload = cast(dict[str, Any], parsed.model_dump(mode="json"))
    trace_payload = original.get("_trace")
    if isinstance(trace_payload, dict):
        payload["_trace"] = trace_payload
    return payload


@workflow.defn(name="DurableActionWorkflow")
class DurableActionWorkflow:
    def __init__(self) -> None:
        self.state = DurableActionState.REQUESTED
        self.external_request_id: str | None = None
        self.message: str | None = None
        self.cancel_requested = False
        self.cancel_reason: str | None = None
        self.last_error: str | None = None
        self.external_status: dict[str, Any] | None = None
        self.input_data: dict[str, Any] = {}

    @workflow.run
    async def run(self, input_data: dict[str, Any]) -> dict[str, Any]:
        parsed = DurableActionInput.model_validate(input_data)
        self.input_data = _validated_payload_with_trace(parsed, input_data)
        await self._transition(DurableActionState.REQUESTED, "tool.action.requested")
        try:
            await self._transition(
                DurableActionState.CREATE_IN_FLIGHT,
                "tool.action.create_started",
            )
            create_result = await workflow.execute_activity(
                create_external_action,
                self.input_data,
                start_to_close_timeout=timedelta(
                    seconds=parsed.tool_config.create.timeout_seconds + 5
                ),
                retry_policy=self._retry_policy(),
            )
            self.external_request_id = create_result.get("external_request_id")
            self.message = create_result.get("message")

            if self.cancel_requested:
                await self._cancel_after_create(parsed)
                return self.status().model_dump(mode="json")

            await self._transition(DurableActionState.ACTIVE, "tool.action.accepted")
            await self._poll_until_terminal(parsed)
        except TimeoutError as exc:
            self.last_error = str(exc)
            await self._transition(DurableActionState.TIMED_OUT, "tool.action.timed_out")
        except Exception as exc:
            self.last_error = str(exc)
            await self._transition(DurableActionState.FAILED, "tool.action.failed")
        return self.status().model_dump(mode="json")

    async def _poll_until_terminal(self, parsed: DurableActionInput) -> None:
        elapsed = 0
        poll_interval = parsed.tool_config.poll_interval_seconds
        max_runtime = parsed.tool_config.max_runtime_seconds
        while elapsed < max_runtime:
            if self.cancel_requested:
                await self._cancel_after_create(parsed)
                return
            try:
                await workflow.wait_condition(
                    lambda: self.cancel_requested or self.external_status is not None,
                    timeout=timedelta(seconds=poll_interval),
                )
            except TimeoutError:
                pass
            elapsed += poll_interval

            status_payload = self.external_status
            self.external_status = None
            if not status_payload and parsed.tool_config.status:
                status_payload = await workflow.execute_activity(
                    get_external_action_status,
                    {**self.input_data, "external_request_id": self.external_request_id},
                    start_to_close_timeout=timedelta(
                        seconds=parsed.tool_config.status.timeout_seconds + 5
                    ),
                    retry_policy=self._retry_policy(),
                )
            if not status_payload:
                continue
            status = str(status_payload.get("status") or "").lower()
            self.message = status_payload.get("message") or self.message
            if status in {"cancelled"}:
                await self._transition(DurableActionState.CANCELLED, "tool.action.cancelled")
                return
            if status in {"cannot_cancel"}:
                await self._transition(
                    DurableActionState.CANNOT_CANCEL,
                    "tool.action.cannot_cancel",
                )
                return
            if status in {"failed", "rejected"}:
                await self._transition(DurableActionState.FAILED, "tool.action.failed")
                return
            if status in set(parsed.tool_config.terminal_states):
                await self._transition(DurableActionState.COMPLETED, "tool.action.completed")
                return
        await self._transition(DurableActionState.TIMED_OUT, "tool.action.timed_out")

    async def _cancel_after_create(self, parsed: DurableActionInput) -> None:
        if not parsed.tool_config.cancel:
            await self._transition(DurableActionState.CANNOT_CANCEL, "tool.action.cannot_cancel")
            return
        await self._transition(DurableActionState.CANCEL_IN_FLIGHT, "tool.action.cancel_requested")
        result = await workflow.execute_activity(
            cancel_external_action,
            {
                **self.input_data,
                "external_request_id": self.external_request_id,
                "cancel_reason": self.cancel_reason,
            },
            start_to_close_timeout=timedelta(seconds=parsed.tool_config.cancel.timeout_seconds + 5),
            retry_policy=self._retry_policy(),
        )
        status = str(result.get("status") or "").lower()
        self.message = result.get("message") or self.message
        if status == "cannot_cancel":
            await self._transition(DurableActionState.CANNOT_CANCEL, "tool.action.cannot_cancel")
        elif status == "failed":
            self.last_error = result.get("message")
            await self._transition(DurableActionState.FAILED, "tool.action.failed")
        else:
            await self._transition(DurableActionState.CANCELLED, "tool.action.cancelled")

    async def _transition(self, state: DurableActionState, event_type: str) -> None:
        self.state = state
        payload = {
            **self.input_data,
            "state": str(self.state),
            "external_request_id": self.external_request_id,
            "message": self.message,
            "cancel_requested": self.cancel_requested,
            "cancel_reason": self.cancel_reason,
            "last_error": self.last_error,
            "workflow_id": workflow.info().workflow_id,
        }
        await workflow.execute_activity(
            persist_tool_state,
            payload,
            start_to_close_timeout=timedelta(seconds=15),
            retry_policy=self._retry_policy(),
        )
        await workflow.execute_activity(
            emit_tool_event,
            {**payload, "event_type": event_type},
            start_to_close_timeout=timedelta(seconds=15),
            retry_policy=self._retry_policy(),
        )

    def _retry_policy(self) -> RetryPolicy:
        return RetryPolicy(
            initial_interval=timedelta(milliseconds=250),
            backoff_coefficient=2,
            maximum_interval=timedelta(seconds=5),
            maximum_attempts=5,
        )

    @workflow.signal(name="CancelRequested")
    async def cancel_requested_signal(self, event: dict[str, Any]) -> None:
        self.cancel_requested = True
        self.cancel_reason = str(event.get("reason") or "requested_by_user")

    @workflow.signal(name="ExternalStatusUpdated")
    async def external_status_updated(self, event: dict[str, Any]) -> None:
        self.external_status = event

    @workflow.query(name="GetDurableActionStatus")
    def status(self) -> DurableActionStatus:
        return DurableActionStatus(
            state=self.state,
            external_request_id=self.external_request_id,
            message=self.message,
            cancel_requested=self.cancel_requested,
            last_error=self.last_error,
        )


@workflow.defn(name="BillingFinalizationWorkflow")
class BillingFinalizationWorkflow:
    def __init__(self) -> None:
        self.state = BillingWorkflowState.WAITING_FOR_CALL_END
        self.call_ended = False
        self.present_usage_types: set[str] = set()
        self.missing_usage_types: set[str] = set()
        self.missing_expectations: set[str] = set()
        self.manifest_present = False
        self.projection_caught_up = False
        self.projection_update_count = 0
        self.warnings: list[str] = []
        self.total_cost_cents: int | None = None
        self.input_data: dict[str, Any] = {}

    @workflow.run
    async def run(self, input_data: dict[str, Any]) -> dict[str, Any]:
        parsed = BillingFinalizationInput.model_validate(input_data)
        self.input_data = _validated_payload_with_trace(parsed, input_data)
        if input_data.get("call_ended"):
            self.call_ended = True
        try:
            await workflow.wait_condition(
                lambda: self.call_ended,
                timeout=timedelta(seconds=parsed.wait_timeout_seconds),
            )
            deadline = parsed.wait_timeout_seconds
            elapsed = 0
            ready = False
            while elapsed <= deadline:
                readiness = await workflow.execute_activity(
                    load_billing_readiness,
                    self.input_data,
                    start_to_close_timeout=timedelta(seconds=15),
                )
                self.manifest_present = bool(readiness.get("manifest_present"))
                self.projection_caught_up = bool(readiness.get("projection_caught_up"))
                self.present_usage_types = set(readiness.get("present_usage_types", []))
                self.missing_usage_types = set(readiness.get("missing_usage_types", []))
                self.missing_expectations = set(readiness.get("missing_expectations", []))
                ready = (
                    self.manifest_present
                    and self.projection_caught_up
                    and not self.missing_usage_types
                    and not self.missing_expectations
                )
                if ready:
                    break
                if not self.manifest_present:
                    self.state = BillingWorkflowState.WAITING_FOR_MANIFEST
                elif not self.projection_caught_up:
                    self.state = BillingWorkflowState.WAITING_FOR_PROJECTION
                else:
                    self.state = BillingWorkflowState.WAITING_FOR_USAGE
                observed_updates = self.projection_update_count
                wait_seconds = max(1, parsed.settle_seconds or 2)
                try:
                    def projection_updated(observed: int = observed_updates) -> bool:
                        return self.projection_update_count > observed

                    await workflow.wait_condition(
                        projection_updated,
                        timeout=timedelta(seconds=wait_seconds),
                    )
                except TimeoutError:
                    pass
                elapsed += wait_seconds

            self.state = BillingWorkflowState.FINALIZING
            result = await workflow.execute_activity(
                finalize_call_billing,
                {
                    **self.input_data,
                    "missing_usage_types": sorted(self.missing_usage_types),
                    "missing_expectations": sorted(self.missing_expectations),
                    "manifest_missing": not self.manifest_present,
                    "projection_caught_up": self.projection_caught_up,
                    "status": str(
                        BillingWorkflowState.FINALIZED
                        if ready
                        else parsed.missing_usage_policy
                    ),
                },
                start_to_close_timeout=timedelta(seconds=20),
            )
            self.total_cost_cents = int(result.get("total_cost_cents", 0))
            self.warnings = list(result.get("warnings", []))
            self.state = BillingWorkflowState(str(result["status"]))
            await workflow.execute_activity(
                emit_workflow_event,
                {
                    **self.input_data,
                    "event_type": "billing.finalized",
                    "workflow_id": workflow.info().workflow_id,
                    "payload": result,
                },
                start_to_close_timeout=timedelta(seconds=15),
            )
        except Exception as exc:
            self.state = BillingWorkflowState.FAILED
            self.warnings.append(str(exc))
        return self.status().model_dump(mode="json")

    @workflow.signal(name="CallEnded")
    async def call_ended_signal(self, event: dict[str, Any]) -> None:
        self.call_ended = True

    @workflow.signal(name="UsageRecorded")
    async def usage_recorded(self, event: dict[str, Any]) -> None:
        usage_type = event.get("usage_type")
        if usage_type:
            self.present_usage_types.add(str(usage_type))
        self.projection_update_count += 1

    @workflow.signal(name="UsageProjectionUpdated")
    async def usage_projection_updated(self, event: dict[str, Any]) -> None:
        self.projection_update_count += 1

    @workflow.query(name="GetBillingState")
    def status(self) -> BillingStatus:
        return BillingStatus(
            state=self.state,
            present_usage_types=sorted(self.present_usage_types),
            missing_usage_types=sorted(self.missing_usage_types),
            manifest_present=self.manifest_present,
            projection_caught_up=self.projection_caught_up,
            missing_expectations=sorted(self.missing_expectations),
            total_cost_cents=self.total_cost_cents,
            warnings=self.warnings,
        )


@workflow.defn(name="BillingAdjustmentWorkflow")
class BillingAdjustmentWorkflow:
    @workflow.run
    async def run(self, input_data: dict[str, Any]) -> dict[str, Any]:
        result = cast(
            dict[str, Any],
            await workflow.execute_activity(
                create_billing_adjustment,
                {**input_data, "workflow_id": workflow.info().workflow_id},
                start_to_close_timeout=timedelta(seconds=20),
            ),
        )
        await workflow.execute_activity(
            emit_workflow_event,
            {
                **input_data,
                "event_type": "billing.adjustment_created",
                "workflow_id": workflow.info().workflow_id,
                "payload": result,
            },
            start_to_close_timeout=timedelta(seconds=15),
        )
        return result


@workflow.defn(name="WebhookDeliveryWorkflow")
class WebhookDeliveryWorkflow:
    def __init__(self) -> None:
        self.state = WebhookDeliveryState.REQUESTED
        self.attempt_count = 0
        self.last_error: str | None = None

    @workflow.run
    async def run(self, input_data: dict[str, Any]) -> dict[str, Any]:
        parsed = WebhookDeliveryInput.model_validate(input_data)
        await workflow.execute_activity(
            persist_webhook_delivery,
            {
                **parsed.model_dump(mode="json"),
                "status": str(self.state),
                "workflow_id": workflow.info().workflow_id,
            },
            start_to_close_timeout=timedelta(seconds=15),
        )
        for attempt in range(1, parsed.max_attempts + 1):
            self.state = WebhookDeliveryState.DELIVERING
            self.attempt_count = attempt
            result = await workflow.execute_activity(
                deliver_webhook,
                {
                    **parsed.model_dump(mode="json"),
                    "attempt_number": attempt,
                    "workflow_id": workflow.info().workflow_id,
                },
                start_to_close_timeout=timedelta(seconds=20),
            )
            if result.get("success"):
                self.state = WebhookDeliveryState.DELIVERED
                await workflow.execute_activity(
                    persist_webhook_delivery,
                    {
                        **parsed.model_dump(mode="json"),
                        "status": str(self.state),
                        "workflow_id": workflow.info().workflow_id,
                        "attempts": attempt,
                        "last_status_code": result.get("status_code"),
                    },
                    start_to_close_timeout=timedelta(seconds=15),
                )
                return {"state": str(self.state), "attempts": attempt}
            self.last_error = str(result.get("error") or "webhook failed")
            if parsed.backoff_seconds:
                await workflow.sleep(timedelta(seconds=parsed.backoff_seconds * attempt))
        self.state = WebhookDeliveryState.FAILED
        await workflow.execute_activity(
            persist_webhook_delivery,
            {
                **parsed.model_dump(mode="json"),
                "status": str(self.state),
                "workflow_id": workflow.info().workflow_id,
                "attempts": self.attempt_count,
                "last_error": self.last_error,
            },
            start_to_close_timeout=timedelta(seconds=15),
        )
        return {"state": str(self.state), "attempts": self.attempt_count}

    @workflow.query(name="GetWebhookDeliveryState")
    def status(self) -> dict[str, Any]:
        return {
            "state": str(self.state),
            "attempt_count": self.attempt_count,
            "last_error": self.last_error,
        }


@workflow.defn(name="CallCompletionWorkflow")
class CallCompletionWorkflow:
    def __init__(self) -> None:
        self.state = "REQUESTED"

    @workflow.run
    async def run(self, input_data: dict[str, Any]) -> dict[str, Any]:
        self.state = "SUMMARIZING"
        summary = await workflow.execute_activity(
            summarize_call,
            input_data,
            start_to_close_timeout=timedelta(seconds=30),
        )
        self.state = "FINALIZING_BILLING"
        billing = await workflow.execute_activity(
            finalize_call_billing,
            {
                **input_data,
                "missing_usage_types": [],
                "status": "FINALIZED",
            },
            start_to_close_timeout=timedelta(seconds=20),
        )
        self.state = "PERSISTING_FINAL_STATE"
        await workflow.execute_activity(
            mark_call_completed,
            {"call_id": input_data["call_id"], "summary": summary},
            start_to_close_timeout=timedelta(seconds=15),
        )
        self.state = "DONE"
        await workflow.execute_activity(
            emit_workflow_event,
            {
                **input_data,
                "event_type": "workflow.done",
                "workflow_id": workflow.info().workflow_id,
                "payload": {"summary": summary, "billing": billing},
            },
            start_to_close_timeout=timedelta(seconds=15),
        )
        return {"state": self.state, "summary": summary, "billing": billing}

    @workflow.query(name="GetCallCompletionState")
    def status(self) -> str:
        return self.state
