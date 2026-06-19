import asyncio
import math
from array import array
from collections.abc import Callable

from apps.api.pipeline.stages.vad import EnergyVADProvider, SmoothedTurnDetector, WebRTCVADProvider


def pcm_frame(sample_count: int, amplitude: int) -> bytes:
    return array("h", [amplitude] * sample_count).tobytes()


def voiced_frame(sample_count: int, amplitude: int, sample_rate: int = 16_000) -> bytes:
    samples = [
        int(
            amplitude
            * (
                0.6 * math.sin(2 * math.pi * 180 * index / sample_rate)
                + 0.25 * math.sin(2 * math.pi * 360 * index / sample_rate)
            )
        )
        for index in range(sample_count)
    ]
    return array("h", samples).tobytes()


async def main() -> None:
    try:
        provider = WebRTCVADProvider(mode=2, sample_rate=16_000, frame_ms=20)
    except RuntimeError:
        provider = EnergyVADProvider(0.018)

    detector = SmoothedTurnDetector(min_speech_ms=200, end_silence_ms=700, speech_pad_ms=150)
    sequence: list[tuple[str, int, Callable[[], bytes]]] = [
        ("silence", 30, lambda: pcm_frame(320, 0)),
        ("background_noise", 30, lambda: pcm_frame(320, 120)),
        ("short_spike", 1, lambda: pcm_frame(320, 8_000)),
        ("silence_after_spike", 15, lambda: pcm_frame(320, 0)),
        ("speech_like", 20, lambda: voiced_frame(320, 4_000)),
        ("endpoint_silence", 80, lambda: pcm_frame(320, 0)),
    ]

    print(f"provider={provider.name}")
    for label, frames, frame_factory in sequence:
        starts = 0
        ends = 0
        for _ in range(frames):
            result = await provider.detect_speech(frame_factory(), 16_000)
            update = detector.update(result)
            starts += int(update.started)
            ends += int(update.ended)
        snapshot = detector.snapshot()
        print(
            f"{label}: starts={starts} ends={ends} "
            f"state={snapshot['state']} speech_ratio={snapshot['speech_frame_ratio']:.2f}"
        )


if __name__ == "__main__":
    asyncio.run(main())
