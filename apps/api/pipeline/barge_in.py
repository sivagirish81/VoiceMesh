import time
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class BargeInState(StrEnum):
    IDLE = "IDLE"
    ASSISTANT_PLAYING = "ASSISTANT_PLAYING"
    CANDIDATE = "CANDIDATE"
    CONFIRMED = "CONFIRMED"
    REJECTED = "REJECTED"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"
    RESOLVING = "RESOLVING"


class BargeInSemantic(StrEnum):
    CORRECTION = "CORRECTION"
    CANCELLATION_REQUEST = "CANCELLATION_REQUEST"
    ADDITIVE_CONTEXT = "ADDITIVE_CONTEXT"
    BACKCHANNEL = "BACKCHANNEL"
    CLARIFICATION_OR_REPEAT = "CLARIFICATION_OR_REPEAT"
    NOISE_OR_ECHO = "NOISE_OR_ECHO"
    UNKNOWN_INTERRUPTION = "UNKNOWN_INTERRUPTION"


@dataclass(frozen=True, slots=True)
class PlaybackCursor:
    turn_id: str
    response_id: str
    last_played_sequence: int = 0
    played_audio_ms: float = 0.0
    updated_at_monotonic: float = 0.0


@dataclass(frozen=True, slots=True)
class BargeInCandidate:
    barge_in_id: str
    call_id: str
    turn_id: str
    response_id: str
    detected_at_monotonic_ms: float
    last_played_sequence: int = 0
    played_audio_ms: float = 0.0
    source: str = "browser"


@dataclass(frozen=True, slots=True)
class BargeInTransition:
    state: BargeInState
    reason_code: str
    barge_in_id: str | None = None
    turn_id: str | None = None
    response_id: str | None = None
    duplicate: bool = False


class BargeInCoordinator:
    """Per-call interruption state.

    The coordinator intentionally performs only local state transitions. Callers decide
    whether to publish Kafka events, update metrics, or cancel providers after a
    transition has been accepted.
    """

    def __init__(self, *, call_id: str, candidate_retention_ms: int) -> None:
        self.call_id = call_id
        self.candidate_retention_ms = candidate_retention_ms
        self.state = BargeInState.IDLE
        self.active_turn_id: str | None = None
        self.active_response_id: str | None = None
        self.current_candidate: BargeInCandidate | None = None
        self.processed_candidate_ids: set[str] = set()
        self.playback_cursor: PlaybackCursor | None = None
        self.interrupted_responses: dict[str, PlaybackCursor] = {}
        self.last_semantic: BargeInSemantic | None = None

    def assistant_playing(self, *, turn_id: str, response_id: str) -> None:
        self.active_turn_id = turn_id
        self.active_response_id = response_id
        if self.state not in {BargeInState.CANDIDATE, BargeInState.CONFIRMED}:
            self.state = BargeInState.ASSISTANT_PLAYING

    def assistant_finished(self, response_id: str) -> BargeInTransition | None:
        if response_id != self.active_response_id:
            return None
        if self.current_candidate and self.current_candidate.response_id == response_id:
            return None
        self.active_turn_id = None
        self.active_response_id = None
        self.playback_cursor = None
        if self.state == BargeInState.ASSISTANT_PLAYING:
            self.state = BargeInState.IDLE
        return BargeInTransition(
            state=self.state,
            reason_code="playback_completed",
            response_id=response_id,
        )

    def playback_progress(
        self,
        *,
        turn_id: str,
        response_id: str,
        last_played_sequence: int,
        played_audio_ms: float,
    ) -> PlaybackCursor:
        cursor = PlaybackCursor(
            turn_id=turn_id,
            response_id=response_id,
            last_played_sequence=max(0, int(last_played_sequence)),
            played_audio_ms=max(0.0, float(played_audio_ms)),
            updated_at_monotonic=time.monotonic(),
        )
        if response_id == self.active_response_id:
            self.playback_cursor = cursor
        return cursor

    def candidate(self, candidate: BargeInCandidate) -> BargeInTransition:
        if candidate.barge_in_id in self.processed_candidate_ids:
            return BargeInTransition(
                state=self.state,
                reason_code="duplicate_candidate",
                barge_in_id=candidate.barge_in_id,
                turn_id=candidate.turn_id,
                response_id=candidate.response_id,
                duplicate=True,
            )
        if candidate.response_id != self.active_response_id:
            self.processed_candidate_ids.add(candidate.barge_in_id)
            return BargeInTransition(
                state=self.state,
                reason_code="stale_response",
                barge_in_id=candidate.barge_in_id,
                turn_id=candidate.turn_id,
                response_id=candidate.response_id,
            )
        self.current_candidate = candidate
        self.state = BargeInState.CANDIDATE
        return BargeInTransition(
            state=self.state,
            reason_code="candidate_detected",
            barge_in_id=candidate.barge_in_id,
            turn_id=candidate.turn_id,
            response_id=candidate.response_id,
        )

    def backend_candidate(self, *, turn_id: str, response_id: str) -> BargeInTransition:
        candidate = BargeInCandidate(
            barge_in_id=f"backend-{response_id}",
            call_id=self.call_id,
            turn_id=turn_id,
            response_id=response_id,
            detected_at_monotonic_ms=time.monotonic() * 1000,
            source="backend_vad",
        )
        return self.candidate(candidate)

    def reject(self, reason_code: str) -> BargeInTransition | None:
        if not self.current_candidate:
            return None
        candidate = self.current_candidate
        self.processed_candidate_ids.add(candidate.barge_in_id)
        self.current_candidate = None
        self.state = BargeInState.REJECTED
        return BargeInTransition(
            state=self.state,
            reason_code=reason_code,
            barge_in_id=candidate.barge_in_id,
            turn_id=candidate.turn_id,
            response_id=candidate.response_id,
        )

    def reject_expired(self) -> BargeInTransition | None:
        candidate = self.current_candidate
        if not candidate:
            return None
        age_ms = time.monotonic() * 1000 - candidate.detected_at_monotonic_ms
        if age_ms < self.candidate_retention_ms:
            return None
        return self.reject("candidate_timeout")

    def confirm(self, reason_code: str = "sustained_speech") -> BargeInTransition | None:
        candidate = self.current_candidate
        if not candidate:
            if not self.active_turn_id or not self.active_response_id:
                return None
            candidate = BargeInCandidate(
                barge_in_id=f"backend-{self.active_response_id}",
                call_id=self.call_id,
                turn_id=self.active_turn_id,
                response_id=self.active_response_id,
                detected_at_monotonic_ms=time.monotonic() * 1000,
                source="backend_vad",
            )
        if candidate.response_id != self.active_response_id:
            return self.reject("stale_response")
        self.current_candidate = candidate
        self.state = BargeInState.CONFIRMED
        return BargeInTransition(
            state=self.state,
            reason_code=reason_code,
            barge_in_id=candidate.barge_in_id,
            turn_id=candidate.turn_id,
            response_id=candidate.response_id,
        )

    def begin_cancelling(self, response_id: str) -> BargeInTransition | None:
        if response_id != self.active_response_id:
            return None
        self.state = BargeInState.CANCELLING
        return BargeInTransition(
            state=self.state,
            reason_code="cancel_started",
            barge_in_id=self.current_candidate.barge_in_id if self.current_candidate else None,
            turn_id=self.active_turn_id,
            response_id=response_id,
        )

    def cancelled(self, response_id: str) -> BargeInTransition | None:
        if response_id != self.active_response_id:
            return None
        cursor = self.playback_cursor or PlaybackCursor(
            turn_id=self.active_turn_id or "unknown",
            response_id=response_id,
            updated_at_monotonic=time.monotonic(),
        )
        self.interrupted_responses[response_id] = cursor
        if self.current_candidate:
            self.processed_candidate_ids.add(self.current_candidate.barge_in_id)
        self.current_candidate = None
        self.state = BargeInState.CANCELLED
        return BargeInTransition(
            state=self.state,
            reason_code="cancelled",
            turn_id=cursor.turn_id,
            response_id=response_id,
        )

    def resolving(self) -> None:
        if self.state in {BargeInState.CONFIRMED, BargeInState.CANCELLED}:
            self.state = BargeInState.RESOLVING

    def resolved(self, semantic: BargeInSemantic) -> None:
        self.last_semantic = semantic
        self.state = BargeInState.IDLE
        self.active_turn_id = None
        self.active_response_id = None
        self.playback_cursor = None

    def snapshot(self) -> dict[str, Any]:
        return {
            "state": self.state.value,
            "active_turn_id": self.active_turn_id,
            "active_response_id": self.active_response_id,
            "candidate_id": self.current_candidate.barge_in_id
            if self.current_candidate
            else None,
            "last_semantic": self.last_semantic.value if self.last_semantic else None,
            "playback": {
                "last_played_sequence": self.playback_cursor.last_played_sequence,
                "played_audio_ms": self.playback_cursor.played_audio_ms,
            }
            if self.playback_cursor
            else None,
        }


def classify_interruption(transcript: str) -> BargeInSemantic:
    cleaned = " ".join(transcript.lower().strip(" \t\n\r.,!?").split())
    if not cleaned:
        return BargeInSemantic.NOISE_OR_ECHO
    backchannels = {"yeah", "yep", "right", "okay", "ok", "mm-hmm", "mhm", "uh huh"}
    if cleaned in backchannels or (len(cleaned.split()) <= 2 and cleaned in backchannels):
        return BargeInSemantic.BACKCHANNEL
    cancellation_markers = (
        "cancel",
        "never mind",
        "nevermind",
        "don't submit",
        "do not submit",
        "stop that",
    )
    if any(marker in cleaned for marker in cancellation_markers):
        return BargeInSemantic.CANCELLATION_REQUEST
    correction_markers = (
        "strike that",
        "scratch that",
        "actually",
        "i meant",
        "no i mean",
        "no, i mean",
        "use",
        "instead",
    )
    if any(marker in cleaned for marker in correction_markers):
        return BargeInSemantic.CORRECTION
    additive_markers = ("also", "and one more", "one more thing", "by the way", "plus")
    if cleaned.startswith("and ") or any(marker in cleaned for marker in additive_markers):
        return BargeInSemantic.ADDITIVE_CONTEXT
    clarification_markers = ("what did you say", "repeat", "say that again", "can you repeat")
    if any(marker in cleaned for marker in clarification_markers):
        return BargeInSemantic.CLARIFICATION_OR_REPEAT
    return BargeInSemantic.UNKNOWN_INTERRUPTION
