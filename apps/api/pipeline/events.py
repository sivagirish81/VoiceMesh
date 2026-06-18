import time
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class AudioFrame:
    data: bytes
    sample_rate: int
    channels: int = 1


@dataclass(frozen=True, slots=True)
class TextChunk:
    call_id: str
    turn_id: str
    response_id: str
    sequence: int
    text: str
    estimated_speech_ms: float
    created_at_monotonic: float = field(default_factory=time.monotonic)


@dataclass(frozen=True, slots=True)
class AudioChunk:
    call_id: str
    turn_id: str
    response_id: str
    sequence: int
    data: bytes
    sample_rate: int
    duration_ms: float
    created_at_monotonic: float = field(default_factory=time.monotonic)


@dataclass(frozen=True, slots=True)
class EndOfStream:
    call_id: str
    turn_id: str
    response_id: str
    reason: str = "completed"


@dataclass(slots=True)
class BackpressureStageState:
    corked: bool = False
    hard_limited: bool = False
    depth: float = 0.0
    depth_unit: str = "items"
    item_count: int = 0
    reason_code: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "corked": self.corked,
            "hard_limited": self.hard_limited,
            "depth": self.depth,
            "depth_unit": self.depth_unit,
            "item_count": self.item_count,
            "reason_code": self.reason_code,
        }


@dataclass(slots=True)
class PipelineState:
    call_id: str
    current_stage: str = "transport"
    corked: bool = False
    cork_reason: str | None = None
    queue_depths: dict[str, float] = field(default_factory=dict)
    backpressure: dict[str, BackpressureStageState] = field(default_factory=dict)
    transcript: str = ""
    response: str = ""
    turn_id: str = "session"
    active_response_id: str | None = None
    sequence_number: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    def backpressure_payload(self) -> dict[str, dict[str, Any]]:
        return {stage: value.as_dict() for stage, value in self.backpressure.items()}
