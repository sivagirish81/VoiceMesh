from array import array

from apps.api.pipeline.audio import resample_pcm16_mono


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
