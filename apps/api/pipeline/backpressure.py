import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Generic, TypeVar

from apps.api.telemetry.metrics import (
    BACKPRESSURE_SECONDS,
    BACKPRESSURE_TOTAL,
    QUEUE_DEPTH,
)

T = TypeVar("T")
StateCallback = Callable[[bool, str, int], Awaitable[None]]


class BackpressureController(Generic[T]):
    def __init__(
        self,
        *,
        call_id: str,
        stage: str,
        high_watermark: int,
        low_watermark: int,
        on_state_change: StateCallback,
    ) -> None:
        self.call_id = call_id
        self.stage = stage
        self.queue: asyncio.Queue[T] = asyncio.Queue(maxsize=high_watermark + 1)
        self.high_watermark = high_watermark
        self.low_watermark = low_watermark
        self.on_state_change = on_state_change
        self.corked = False
        self._corked_at: float | None = None
        self._lock = asyncio.Lock()

    @property
    def depth(self) -> int:
        return self.queue.qsize()

    async def put(self, item: T) -> None:
        await self.queue.put(item)
        await self._observe()

    async def get(self) -> T:
        item = await self.queue.get()
        await self._observe()
        return item

    def task_done(self) -> None:
        self.queue.task_done()

    async def _observe(self) -> None:
        depth = self.depth
        QUEUE_DEPTH.labels(self.call_id, self.stage).set(depth)
        async with self._lock:
            if not self.corked and depth >= self.high_watermark:
                self.corked = True
                self._corked_at = time.monotonic()
                reason = f"{self.stage} queue depth {depth} reached high watermark"
                BACKPRESSURE_TOTAL.labels(reason).inc()
                await self.on_state_change(True, reason, depth)
            elif self.corked and depth <= self.low_watermark:
                reason = f"{self.stage} queue drained to low watermark"
                if self._corked_at is not None:
                    BACKPRESSURE_SECONDS.labels(self.stage).observe(
                        time.monotonic() - self._corked_at
                    )
                self.corked = False
                self._corked_at = None
                await self.on_state_change(False, reason, depth)

