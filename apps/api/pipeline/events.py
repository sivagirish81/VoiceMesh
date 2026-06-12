from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class AudioFrame:
    data: bytes
    sample_rate: int
    channels: int = 1


@dataclass(slots=True)
class FinalizedTurn:
    turn_id: str
    pcm_bytes: bytes
    sample_rate: int


@dataclass(slots=True)
class PipelineState:
    call_id: str
    current_stage: str = "transport"
    corked: bool = False
    cork_reason: str | None = None
    queue_depths: dict[str, int] = field(default_factory=dict)
    transcript: str = ""
    response: str = ""
    turn_id: str = "session"
    sequence_number: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

