import sys
from array import array


def pcm16_sample_count(audio_chunk: bytes) -> int:
    if len(audio_chunk) % 2:
        raise ValueError("PCM16 audio must contain complete 16-bit samples")
    return len(audio_chunk) // 2


def pcm16_duration_ms(audio_chunk: bytes, sample_rate: int) -> float:
    if sample_rate <= 0:
        raise ValueError("sample_rate must be positive")
    return pcm16_sample_count(audio_chunk) / sample_rate * 1000


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


def split_pcm16_frames(audio_chunk: bytes, sample_rate: int, frame_ms: int) -> list[bytes]:
    if frame_ms not in {10, 20, 30}:
        raise ValueError("frame_ms must be one of 10, 20, or 30")
    samples_per_frame = sample_rate * frame_ms // 1000
    bytes_per_frame = samples_per_frame * 2
    if samples_per_frame <= 0 or len(audio_chunk) < bytes_per_frame:
        return []
    complete_bytes = len(audio_chunk) - (len(audio_chunk) % bytes_per_frame)
    return [
        audio_chunk[offset : offset + bytes_per_frame]
        for offset in range(0, complete_bytes, bytes_per_frame)
    ]
