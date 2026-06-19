from array import array

import pytest

from apps.api.pipeline.audio import (
    pcm16_duration_ms,
    pcm16_sample_count,
    resample_pcm16_mono,
    split_pcm16_frames,
)


def test_resample_48khz_pcm_to_realtime_24khz() -> None:
    source = array("h", range(480))
    result = resample_pcm16_mono(source.tobytes(), source_rate=48_000)
    output = array("h")
    output.frombytes(result)
    assert len(output) == 240
    assert output[0] == source[0]


def test_resample_preserves_24khz_pcm() -> None:
    source = array("h", [1, -2, 3, -4]).tobytes()
    assert resample_pcm16_mono(source, source_rate=24_000) == source


def test_pcm16_duration_ms() -> None:
    source = array("h", [0] * 160).tobytes()

    assert pcm16_sample_count(source) == 160
    assert pcm16_duration_ms(source, 16_000) == 10


def test_pcm16_rejects_malformed_audio() -> None:
    with pytest.raises(ValueError, match="complete 16-bit samples"):
        pcm16_sample_count(b"\x00")


def test_split_pcm16_frames_uses_complete_webrtc_frames() -> None:
    source = array("h", [0] * 1_000).tobytes()

    frames = split_pcm16_frames(source, sample_rate=16_000, frame_ms=20)

    assert len(frames) == 3
    assert all(len(frame) == 640 for frame in frames)


def test_split_pcm16_frames_rejects_invalid_frame_ms() -> None:
    with pytest.raises(ValueError, match="10, 20, or 30"):
        split_pcm16_frames(b"\x00" * 640, sample_rate=16_000, frame_ms=15)
