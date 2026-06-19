import asyncio
from array import array

import pytest

from apps.api.pipeline.stages.vad import (
    EnergyVADProvider,
    SmoothedTurnDetector,
    VADState,
    WebRTCVADProvider,
)
from apps.api.providers.base import VADResult


def pcm_frame(sample_count: int, amplitude: int) -> bytes:
    return array("h", [amplitude] * sample_count).tobytes()


def test_energy_vad_returns_rich_result_and_tracks_noise_floor() -> None:
    provider = EnergyVADProvider(
        0.01,
        adaptive_noise_floor=True,
        noise_multiplier=3.0,
        noise_update_alpha=0.5,
    )

    noise = asyncio.run(provider.detect_speech(pcm_frame(160, 100), 16_000))
    speech = asyncio.run(provider.detect_speech(pcm_frame(160, 2_000), 16_000))

    assert noise.provider == "energy"
    assert not noise.speech
    assert noise.noise_floor is not None
    assert speech.speech
    assert speech.energy is not None
    assert speech.energy > noise.energy


def test_webrtc_vad_accepts_valid_pcm_frames() -> None:
    pytest.importorskip("webrtcvad")
    provider = WebRTCVADProvider(mode=2, sample_rate=16_000, frame_ms=20)

    result = asyncio.run(provider.detect_speech(pcm_frame(320, 0), 16_000))

    assert result.provider == "webrtc"
    assert result.sample_rate == 16_000
    assert result.frame_duration_ms == 20
    assert result.probability == 0
    assert not result.speech


def test_webrtc_vad_safely_handles_incomplete_frames() -> None:
    pytest.importorskip("webrtcvad")
    provider = WebRTCVADProvider(mode=3, sample_rate=16_000, frame_ms=20)

    result = asyncio.run(provider.detect_speech(pcm_frame(100, 0), 16_000))

    assert not result.speech
    assert result.reason == "no_complete_webrtc_frame"


def test_webrtc_vad_rejects_invalid_mode() -> None:
    with pytest.raises(ValueError, match="WEBRTC_VAD_MODE"):
        WebRTCVADProvider(mode=4, sample_rate=16_000, frame_ms=20)


def vad_result(speech: bool, duration_ms: float = 20) -> VADResult:
    return VADResult(
        speech=speech,
        probability=1.0 if speech else 0.0,
        energy=0.1 if speech else 0.0,
        noise_floor=0.01,
        sample_rate=16_000,
        frame_duration_ms=duration_ms,
        provider="test",
    )


def test_turn_detector_does_not_start_on_one_noise_spike() -> None:
    detector = SmoothedTurnDetector(min_speech_ms=60, end_silence_ms=60, speech_pad_ms=20)

    first = detector.update(vad_result(True))
    second = detector.update(vad_result(False))

    assert first.state == VADState.STARTING
    assert not first.started
    assert second.state == VADState.QUIET
    assert not second.ended


def test_turn_detector_starts_after_sustained_speech() -> None:
    detector = SmoothedTurnDetector(min_speech_ms=60, end_silence_ms=60, speech_pad_ms=20)

    updates = [detector.update(vad_result(True)) for _ in range(3)]

    assert updates[-1].started
    assert updates[-1].state == VADState.SPEAKING
    assert updates[-1].speech_duration_ms == 60


def test_turn_detector_does_not_end_on_one_silent_frame() -> None:
    detector = SmoothedTurnDetector(min_speech_ms=40, end_silence_ms=60, speech_pad_ms=20)
    detector.update(vad_result(True))
    detector.update(vad_result(True))

    update = detector.update(vad_result(False))

    assert update.state == VADState.STOPPING
    assert not update.ended


def test_turn_detector_ends_after_sustained_silence() -> None:
    detector = SmoothedTurnDetector(min_speech_ms=40, end_silence_ms=60, speech_pad_ms=20)
    detector.update(vad_result(True))
    detector.update(vad_result(True))
    detector.update(vad_result(False))
    detector.update(vad_result(False))

    update = detector.update(vad_result(False))

    assert update.ended
    assert update.state == VADState.QUIET
    assert update.speech_frame_ratio == pytest.approx(2 / 5)
