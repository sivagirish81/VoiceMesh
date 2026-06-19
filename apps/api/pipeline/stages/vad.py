import math
from array import array
from dataclasses import dataclass
from enum import StrEnum
from importlib import import_module

from apps.api.pipeline.audio import (
    pcm16_duration_ms,
    resample_pcm16_mono,
    split_pcm16_frames,
)
from apps.api.providers.base import VADProvider, VADResult


def _pcm_energy(audio_chunk: bytes) -> float:
    if len(audio_chunk) < 2 or len(audio_chunk) % 2:
        return 0.0
    samples = array("h")
    samples.frombytes(audio_chunk)
    if not samples:
        return 0.0
    rms = math.sqrt(sum(sample * sample for sample in samples) / len(samples))
    return rms / 32768.0


class EnergyVADProvider(VADProvider):
    """Adaptive energy VAD fallback for signed 16-bit mono microphone PCM."""

    name = "energy"

    def __init__(
        self,
        threshold: float,
        *,
        adaptive_noise_floor: bool = True,
        noise_multiplier: float = 3.0,
        noise_update_alpha: float = 0.05,
    ) -> None:
        self.threshold = threshold
        self.adaptive_noise_floor = adaptive_noise_floor
        self.noise_multiplier = noise_multiplier
        self.noise_update_alpha = noise_update_alpha
        self.noise_floor: float | None = None

    async def detect_speech(self, audio_chunk: bytes, sample_rate: int) -> VADResult:
        energy = _pcm_energy(audio_chunk)
        floor = self.noise_floor
        effective_threshold = self.threshold
        if self.adaptive_noise_floor:
            if floor is not None:
                effective_threshold = max(self.threshold, floor * self.noise_multiplier)
            speech = energy >= effective_threshold
            if not speech:
                self.noise_floor = (
                    energy
                    if floor is None
                    else (1 - self.noise_update_alpha) * floor
                    + self.noise_update_alpha * energy
                )
        else:
            speech = energy >= effective_threshold
        return VADResult(
            speech=speech,
            probability=None,
            energy=energy,
            noise_floor=self.noise_floor,
            sample_rate=sample_rate,
            frame_duration_ms=pcm16_duration_ms(audio_chunk, sample_rate),
            provider=self.name,
            reason="energy_threshold",
        )


class WebRTCVADProvider(VADProvider):
    name = "webrtc"

    def __init__(self, *, mode: int = 2, sample_rate: int = 16_000, frame_ms: int = 20) -> None:
        if mode not in {0, 1, 2, 3}:
            raise ValueError("WEBRTC_VAD_MODE must be 0, 1, 2, or 3")
        if sample_rate not in {8000, 16000, 32000, 48000}:
            raise ValueError("VAD_SAMPLE_RATE must be one of 8000, 16000, 32000, or 48000")
        if frame_ms not in {10, 20, 30}:
            raise ValueError("VAD_FRAME_MS must be 10, 20, or 30")
        try:
            webrtcvad = import_module("webrtcvad")
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "VAD_PROVIDER=webrtc requires the 'webrtcvad-wheels' package. "
                "Install project dependencies or set VAD_PROVIDER=energy."
            ) from exc
        self.mode = mode
        self.sample_rate = sample_rate
        self.frame_ms = frame_ms
        self._vad = webrtcvad.Vad(mode)

    async def detect_speech(self, audio_chunk: bytes, sample_rate: int) -> VADResult:
        duration_ms = pcm16_duration_ms(audio_chunk, sample_rate)
        energy = _pcm_energy(audio_chunk)
        vad_pcm = (
            resample_pcm16_mono(audio_chunk, source_rate=sample_rate, target_rate=self.sample_rate)
            if sample_rate != self.sample_rate
            else audio_chunk
        )
        frames = split_pcm16_frames(vad_pcm, self.sample_rate, self.frame_ms)
        if not frames:
            return VADResult(
                speech=False,
                probability=0.0,
                energy=energy,
                noise_floor=None,
                sample_rate=self.sample_rate,
                frame_duration_ms=duration_ms,
                provider=self.name,
                reason="no_complete_webrtc_frame",
            )
        speech_frames = sum(1 for frame in frames if self._vad.is_speech(frame, self.sample_rate))
        speech_ratio = speech_frames / len(frames)
        return VADResult(
            speech=speech_ratio >= 0.5,
            probability=speech_ratio,
            energy=energy,
            noise_floor=None,
            sample_rate=self.sample_rate,
            frame_duration_ms=duration_ms,
            provider=self.name,
            reason="webrtc_majority",
        )


class SileroVADProvider(VADProvider):
    name = "silero"

    async def detect_speech(self, audio_chunk: bytes, sample_rate: int) -> VADResult:
        raise RuntimeError(
            "Silero VAD is an optional future provider and is not bundled in this pass. "
            "Use VAD_PROVIDER=webrtc or VAD_PROVIDER=energy."
        )


class VADState(StrEnum):
    QUIET = "quiet"
    STARTING = "starting"
    SPEAKING = "speaking"
    STOPPING = "stopping"


@dataclass(frozen=True, slots=True)
class TurnUpdate:
    previous_state: VADState
    state: VADState
    started: bool = False
    ended: bool = False
    collect_audio: bool = False
    state_changed: bool = False
    speech_duration_ms: float = 0.0
    total_duration_ms: float = 0.0
    speech_frame_ratio: float = 0.0


class SmoothedTurnDetector:
    def __init__(
        self,
        *,
        min_speech_ms: int,
        end_silence_ms: int,
        speech_pad_ms: int,
    ) -> None:
        self.min_speech_ms = min_speech_ms
        self.end_silence_ms = end_silence_ms
        self.speech_pad_ms = speech_pad_ms
        self.state = VADState.QUIET
        self.speech_ms = 0.0
        self.silence_ms = 0.0
        self.turn_speech_ms = 0.0
        self.turn_total_ms = 0.0
        self.turn_speech_frames = 0
        self.turn_total_frames = 0

    def update(self, result: VADResult) -> TurnUpdate:
        previous = self.state
        started = False
        ended = False
        duration = result.frame_duration_ms
        if result.speech:
            self.speech_ms += duration
            self.silence_ms = 0.0
            if self.state == VADState.QUIET:
                self.state = VADState.STARTING
                self._reset_turn_stats()
            elif self.state == VADState.STOPPING:
                self.state = VADState.SPEAKING
            if self.state == VADState.STARTING and self.speech_ms >= self.min_speech_ms:
                self.state = VADState.SPEAKING
                started = True
        else:
            self.speech_ms = 0.0
            if self.state == VADState.STARTING:
                self.state = VADState.QUIET
                self._reset_turn_stats()
            elif self.state == VADState.SPEAKING:
                self.state = VADState.STOPPING
                self.silence_ms = duration
            elif self.state == VADState.STOPPING:
                self.silence_ms += duration
                if self.silence_ms >= self.end_silence_ms:
                    self.state = VADState.QUIET
                    ended = True
                    self.silence_ms = 0.0
        collect_audio = self.state in {VADState.STARTING, VADState.SPEAKING, VADState.STOPPING}
        if collect_audio or ended:
            self.turn_total_ms += duration
            self.turn_total_frames += 1
            if result.speech:
                self.turn_speech_ms += duration
                self.turn_speech_frames += 1
        ratio = self.turn_speech_frames / self.turn_total_frames if self.turn_total_frames else 0.0
        return TurnUpdate(
            previous_state=previous,
            state=self.state,
            started=started,
            ended=ended,
            collect_audio=collect_audio,
            state_changed=previous != self.state,
            speech_duration_ms=self.turn_speech_ms,
            total_duration_ms=self.turn_total_ms,
            speech_frame_ratio=ratio,
        )

    def snapshot(self) -> dict[str, float | str]:
        return {
            "state": self.state,
            "speech_duration_ms": self.turn_speech_ms,
            "total_duration_ms": self.turn_total_ms,
            "speech_frame_ratio": (
                self.turn_speech_frames / self.turn_total_frames if self.turn_total_frames else 0.0
            ),
        }

    def _reset_turn_stats(self) -> None:
        self.turn_speech_ms = 0.0
        self.turn_total_ms = 0.0
        self.turn_speech_frames = 0
        self.turn_total_frames = 0


TurnDetector = SmoothedTurnDetector
