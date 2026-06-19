# VAD And Endpointing

VoiceMesh keeps voice activity detection in the live session worker. Kafka, Postgres,
Temporal, Prometheus, Grafana, Jaeger, and future analytics stores may observe VAD
behavior, but they do not decide whether speech started or ended.

## Current Flow

```text
browser mic constraints
  -> signed 16-bit PCM over WebSocket
  -> VAD audio copy normalized/resampled to 16 kHz
  -> pluggable VAD provider
  -> smoothed turn detector
  -> STT guardrails
  -> streaming STT commit
```

The browser requests `echoCancellation`, `noiseSuppression`, and `autoGainControl` where
supported. These constraints are best-effort browser behavior; they are not a complete
speech detector.

## Providers

`VAD_PROVIDER=webrtc` is the default. It uses WebRTC VAD over 10, 20, or 30 ms PCM frames.
The provider accepts mono signed 16-bit PCM, resamples a copy to the configured VAD sample
rate, splits complete frames, and returns a `VADResult` with speech decision, probability
ratio, energy, sample rate, frame duration, provider name, and reason.

`VAD_PROVIDER=energy` keeps the simple fallback path, but it now tracks an adaptive noise
floor while the detector believes input is not speech:

```text
effective_threshold = max(ENERGY_VAD_MIN_THRESHOLD, noise_floor * ENERGY_VAD_NOISE_MULTIPLIER)
```

`VAD_PROVIDER=silero` is reserved as a future neural VAD extension point. It is not bundled
or required for local setup.

## Endpointing

VoiceMesh no longer starts or ends a turn on a single frame. The turn detector has four
states:

```text
QUIET -> STARTING -> SPEAKING -> STOPPING -> QUIET
```

`STARTING` requires sustained speech for `VAD_MIN_SPEECH_MS` before `vad.speech_started`
is emitted. `STOPPING` requires sustained silence for `VAD_END_SILENCE_MS` before
`vad.speech_ended` finalizes the user turn. This prevents one train-noise spike from
creating a fake turn and prevents one quiet frame from ending a real sentence.

## STT Guardrails

Even after endpointing, the session worker can suppress weak turns before LLM/TTS:

- `too_short`: accepted speech duration is below `VAD_MIN_TURN_AUDIO_MS`;
- `low_speech_ratio`: speech frames are too sparse across the turn;
- `empty_transcript`: STT returns only whitespace.

Ignored turns emit `vad.noise_turn_ignored` and increment Prometheus counters. They do not
call the LLM and do not synthesize audio.

## Tuning

Use lower `WEBRTC_VAD_MODE` values when quiet speakers are being missed. This is more
sensitive and may create more false starts.

Use higher `WEBRTC_VAD_MODE` values in noisy spaces. This is more aggressive and may miss
soft speech.

Increase `VAD_MIN_SPEECH_MS` to reject short spikes. This may make speech start feel a bit
slower.

Increase `VAD_END_SILENCE_MS` to avoid premature turn endings. This increases response
latency after the user stops speaking.

Increase `VAD_MIN_SPEECH_FRAME_RATIO` to reject choppy/noisy turns. This may reject weak
speech in bad network or microphone conditions.

## Debugging

The demo page shows:

- browser-reported echo cancellation, noise suppression, AGC, sample rate, and channels;
- VAD provider, state, latest decision, energy, noise floor, speech duration, and speech
  frame ratio;
- ignored noise turn count.

Jaeger spans named `pipeline.vad` include `call_id`, `turn_id`, provider, VAD decision,
energy, noise floor, state, frame duration, speech-start/end flags, and speech ratio.

Prometheus metrics use stable labels only:

- `voicemesh_vad_frames_total{provider,decision}`;
- `voicemesh_vad_state_transitions_total{provider,from_state,to_state}`;
- `voicemesh_vad_noise_turns_ignored_total{provider,reason_code}`;
- `voicemesh_vad_turn_duration_seconds{provider,outcome}`;
- `voicemesh_vad_endpoint_delay_seconds{provider}`.

Run a deterministic local sanity check:

```bash
make demo-noise-vad
```

## Known Limits

WebRTC VAD is lightweight and fast, but it is not magic. Very noisy mobile environments
may still require calibrated transport audio processing, provider-native endpointing,
or a future neural VAD such as Silero. The current browser transport uses
`ScriptProcessorNode`; an AudioWorklet would give tighter timing and cleaner buffering.
