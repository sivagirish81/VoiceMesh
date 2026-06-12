from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class CallRecord:
    call_id: str
    status: str
    started_at: datetime | None
    ended_at: datetime | None
    current_stage: str
    corked: bool
    cork_reason: str | None
    selected_stt_provider: str
    selected_llm_provider: str
    selected_tts_provider: str
    final_summary: str | None
    error: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(slots=True)
class PersistResult:
    inserted: bool
    duplicate: bool
    payload: dict[str, Any] | None = None

