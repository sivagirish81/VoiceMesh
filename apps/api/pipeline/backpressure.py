import asyncio
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Generic, TypeVar

from apps.api.telemetry.metrics import (
    BACKPRESSURE_SECONDS,
    BACKPRESSURE_TOTAL,
    HARD_LIMIT_TOTAL,
    QUEUE_DEPTH,
    QUEUE_ITEMS,
)

T = TypeVar("T")


class DepthUnit(StrEnum):
    ITEMS = "items"
    SPEAK_AHEAD_MS = "speak_ahead_ms"
    AUDIO_MS = "audio_ms"
    BYTES = "bytes"


@dataclass(frozen=True, slots=True)
class BackpressureTransition:
    call_id: str
    stage: str
    corked: bool
    reason_code: str
    depth: float
    depth_unit: str
    hard_limited: bool = False
    item_count: int = 0

    @property
    def transition(self) -> str:
        if self.hard_limited:
            return "hard_limit"
        return "corked" if self.corked else "uncorked"

    @property
    def reason(self) -> str:
        if self.reason_code == "queue_high_watermark":
            return (
                f"{self.stage} depth {self.depth:.0f} {self.depth_unit} "
                "reached high watermark"
            )
        if self.reason_code == "queue_low_watermark":
            return (
                f"{self.stage} depth {self.depth:.0f} {self.depth_unit} "
                "drained to low watermark"
            )
        if self.reason_code == "queue_hard_limit":
            return (
                f"{self.stage} depth {self.depth:.0f} {self.depth_unit} "
                "reached hard limit"
            )
        return self.reason_code


StateCallback = Callable[[BackpressureTransition], Awaitable[None]]
WeightFn = Callable[[T], float]


class FlowControlledQueue(Generic[T]):
    def __init__(
        self,
        *,
        call_id: str,
        stage: str,
        depth_unit: DepthUnit | str,
        low_watermark: float,
        high_watermark: float,
        hard_limit: float,
        on_state_change: StateCallback,
        weight_fn: WeightFn[T] | None = None,
        max_items: int = 128,
        hard_limit_policy: str = "cancel_response",
    ) -> None:
        if low_watermark >= high_watermark:
            raise ValueError("low_watermark must be lower than high_watermark")
        if high_watermark > hard_limit:
            raise ValueError("high_watermark must be lower than or equal to hard_limit")
        self.call_id = call_id
        self.stage = stage
        self.depth_unit = str(depth_unit)
        self.low_watermark = low_watermark
        self.high_watermark = high_watermark
        self.hard_limit = hard_limit
        self.on_state_change = on_state_change
        self.weight_fn = weight_fn or (lambda _item: 1.0)
        self.max_items = max_items
        self.hard_limit_policy = hard_limit_policy
        self.corked = False
        self.hard_limited = False
        self._depth_weight = 0.0
        self._corked_at: float | None = None
        self._items: deque[tuple[T, float]] = deque()
        self._condition = asyncio.Condition()
        self._closed = False
        self._cancelled = False

    @property
    def depth_items(self) -> int:
        return len(self._items)

    @property
    def depth_weight(self) -> float:
        return self._depth_weight

    @property
    def depth(self) -> float:
        return self._depth_weight

    @property
    def closed(self) -> bool:
        return self._closed

    async def put(self, item: T, *, bypass_cork: bool = False) -> None:
        if not bypass_cork:
            await self.wait_if_corked()
        transition: BackpressureTransition | None = None
        weight = max(0.0, float(self.weight_fn(item)))
        async with self._condition:
            while (
                not self._closed
                and not self._cancelled
                and len(self._items) >= self.max_items
            ):
                await self._condition.wait()
            if self._closed or self._cancelled:
                return
            self._items.append((item, weight))
            self._depth_weight += weight
            transition = self._observe_locked()
            self._condition.notify_all()
        if transition:
            await self.on_state_change(transition)

    async def get(self) -> T:
        transition: BackpressureTransition | None = None
        async with self._condition:
            while not self._items and not self._closed and not self._cancelled:
                await self._condition.wait()
            if not self._items:
                raise QueueClosed(self.stage)
            item, weight = self._items.popleft()
            self._depth_weight = max(0.0, self._depth_weight - weight)
            transition = self._observe_locked()
            self._condition.notify_all()
        if transition:
            await self.on_state_change(transition)
        return item

    def task_done(self) -> None:
        return None

    async def close(self) -> None:
        async with self._condition:
            self._closed = True
            self._condition.notify_all()

    async def cancel(self) -> None:
        async with self._condition:
            self._cancelled = True
            self._closed = True
            self._items.clear()
            self._depth_weight = 0.0
            self.corked = False
            self.hard_limited = False
            self._corked_at = None
            self._observe_metrics_locked()
            self._condition.notify_all()

    async def flush(self, predicate: Callable[[T], bool]) -> list[T]:
        removed: list[T] = []
        transition: BackpressureTransition | None = None
        async with self._condition:
            kept: deque[tuple[T, float]] = deque()
            new_depth = 0.0
            for item, weight in self._items:
                if predicate(item):
                    removed.append(item)
                else:
                    kept.append((item, weight))
                    new_depth += weight
            self._items = kept
            self._depth_weight = new_depth
            transition = self._observe_locked()
            self._condition.notify_all()
        if transition:
            await self.on_state_change(transition)
        return removed

    async def wait_if_corked(self) -> None:
        await self.wait_until_uncorked()

    async def wait_until_uncorked(self) -> None:
        async with self._condition:
            while self.corked and not self._closed and not self._cancelled:
                await self._condition.wait()

    def _observe_locked(self) -> BackpressureTransition | None:
        self._observe_metrics_locked()
        transition: BackpressureTransition | None = None
        if self._depth_weight >= self.hard_limit and not self.hard_limited:
            self.hard_limited = True
            self.corked = True
            self._corked_at = self._corked_at or time.monotonic()
            transition = BackpressureTransition(
                call_id=self.call_id,
                stage=self.stage,
                corked=True,
                reason_code="queue_hard_limit",
                depth=self._depth_weight,
                depth_unit=self.depth_unit,
                hard_limited=True,
                item_count=len(self._items),
            )
            HARD_LIMIT_TOTAL.labels(self.stage, self.depth_unit, self.hard_limit_policy).inc()
        elif not self.corked and self._depth_weight >= self.high_watermark:
            self.corked = True
            self._corked_at = time.monotonic()
            transition = BackpressureTransition(
                call_id=self.call_id,
                stage=self.stage,
                corked=True,
                reason_code="queue_high_watermark",
                depth=self._depth_weight,
                depth_unit=self.depth_unit,
                item_count=len(self._items),
            )
            BACKPRESSURE_TOTAL.labels(
                self.stage, "corked", "queue_high_watermark", self.depth_unit
            ).inc()
        elif self.corked and self._depth_weight <= self.low_watermark:
            if self._corked_at is not None:
                BACKPRESSURE_SECONDS.labels(self.stage).observe(
                    time.monotonic() - self._corked_at
                )
            self.corked = False
            self.hard_limited = False
            self._corked_at = None
            transition = BackpressureTransition(
                call_id=self.call_id,
                stage=self.stage,
                corked=False,
                reason_code="queue_low_watermark",
                depth=self._depth_weight,
                depth_unit=self.depth_unit,
                item_count=len(self._items),
            )
            BACKPRESSURE_TOTAL.labels(
                self.stage, "uncorked", "queue_low_watermark", self.depth_unit
            ).inc()
        self._condition.notify_all()
        return transition

    def _observe_metrics_locked(self) -> None:
        QUEUE_DEPTH.labels(self.stage, self.depth_unit).set(self._depth_weight)
        QUEUE_ITEMS.labels(self.stage).set(len(self._items))


class QueueClosed(RuntimeError):
    def __init__(self, stage: str) -> None:
        super().__init__(f"{stage} queue is closed")


BackpressureController = FlowControlledQueue
