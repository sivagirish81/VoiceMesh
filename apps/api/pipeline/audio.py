import sys
from array import array


def resample_pcm16_mono(
    audio_chunk: bytes, source_rate: int, target_rate: int = 24_000
) -> bytes:
    if source_rate <= 0:
        raise ValueError("source_rate must be positive")
    if len(audio_chunk) % 2:
        raise ValueError("PCM16 audio must contain complete 16-bit samples")
    if source_rate == target_rate:
        return audio_chunk

    samples = array("h")
    samples.frombytes(audio_chunk)
    if sys.byteorder != "little":
        samples.byteswap()
    if len(samples) < 2:
        return audio_chunk

    output_length = max(1, round(len(samples) * target_rate / source_rate))
    scale = source_rate / target_rate
    output = array("h")
    for output_index in range(output_length):
        source_position = min(output_index * scale, len(samples) - 1)
        left = int(source_position)
        right = min(left + 1, len(samples) - 1)
        fraction = source_position - left
        value = round(samples[left] + (samples[right] - samples[left]) * fraction)
        output.append(max(-32768, min(32767, value)))

    if sys.byteorder != "little":
        output.byteswap()
    return output.tobytes()
