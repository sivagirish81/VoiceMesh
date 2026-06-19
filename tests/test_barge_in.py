import pytest

from apps.api.pipeline.barge_in import (
    BargeInCandidate,
    BargeInCoordinator,
    BargeInSemantic,
    classify_interruption,
)


def candidate(response_id: str = "response-1", barge_in_id: str = "bi-1") -> BargeInCandidate:
    return BargeInCandidate(
        barge_in_id=barge_in_id,
        call_id="call-1",
        turn_id="turn-1",
        response_id=response_id,
        detected_at_monotonic_ms=0,
        last_played_sequence=3,
        played_audio_ms=420,
    )


def test_candidate_is_speculative_until_confirmed() -> None:
    coordinator = BargeInCoordinator(call_id="call-1", candidate_retention_ms=1200)
    coordinator.assistant_playing(turn_id="turn-1", response_id="response-1")

    transition = coordinator.candidate(candidate())

    assert transition.reason_code == "candidate_detected"
    assert coordinator.state == "CANDIDATE"
    assert "response-1" not in coordinator.interrupted_responses


def test_duplicate_candidate_is_idempotent() -> None:
    coordinator = BargeInCoordinator(call_id="call-1", candidate_retention_ms=1200)
    coordinator.assistant_playing(turn_id="turn-1", response_id="response-1")

    coordinator.candidate(candidate())
    coordinator.reject("noise_spike")
    duplicate = coordinator.candidate(candidate())

    assert duplicate.duplicate is True
    assert duplicate.reason_code == "duplicate_candidate"


def test_stale_candidate_is_rejected_without_state_change() -> None:
    coordinator = BargeInCoordinator(call_id="call-1", candidate_retention_ms=1200)
    coordinator.assistant_playing(turn_id="turn-1", response_id="active")

    transition = coordinator.candidate(candidate(response_id="old"))

    assert transition.reason_code == "stale_response"
    assert coordinator.state == "ASSISTANT_PLAYING"


def test_confirmed_candidate_records_playback_cursor_on_cancel() -> None:
    coordinator = BargeInCoordinator(call_id="call-1", candidate_retention_ms=1200)
    coordinator.assistant_playing(turn_id="turn-1", response_id="response-1")
    coordinator.candidate(candidate())
    coordinator.playback_progress(
        turn_id="turn-1",
        response_id="response-1",
        last_played_sequence=7,
        played_audio_ms=900,
    )

    assert coordinator.confirm("sustained_speech") is not None
    assert coordinator.begin_cancelling("response-1") is not None
    assert coordinator.cancelled("response-1") is not None

    assert coordinator.interrupted_responses["response-1"].played_audio_ms == 900


def test_playback_done_clears_assistant_playing_state() -> None:
    coordinator = BargeInCoordinator(call_id="call-1", candidate_retention_ms=1200)
    coordinator.assistant_playing(turn_id="turn-1", response_id="response-1")

    transition = coordinator.assistant_finished("response-1")

    assert transition is not None
    assert transition.reason_code == "playback_completed"
    assert coordinator.state == "IDLE"
    assert coordinator.active_response_id is None


def test_playback_done_does_not_clear_pending_candidate() -> None:
    coordinator = BargeInCoordinator(call_id="call-1", candidate_retention_ms=1200)
    coordinator.assistant_playing(turn_id="turn-1", response_id="response-1")
    coordinator.candidate(candidate())

    assert coordinator.assistant_finished("response-1") is None
    assert coordinator.state == "CANDIDATE"
    assert coordinator.active_response_id == "response-1"


@pytest.mark.parametrize(
    ("transcript", "semantic"),
    [
        ("Strike that, use my work email.", BargeInSemantic.CORRECTION),
        ("Also send it to my work email.", BargeInSemantic.ADDITIVE_CONTEXT),
        ("Cancel the refund.", BargeInSemantic.CANCELLATION_REQUEST),
        ("Yeah.", BargeInSemantic.BACKCHANNEL),
        ("Can you repeat that?", BargeInSemantic.CLARIFICATION_OR_REPEAT),
        ("", BargeInSemantic.NOISE_OR_ECHO),
    ],
)
def test_semantic_classification(transcript: str, semantic: BargeInSemantic) -> None:
    assert classify_interruption(transcript) == semantic
