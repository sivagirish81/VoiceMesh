# Provider Abstractions

The pipeline depends on four interfaces:

- `VADProvider.detect_speech`
- `STTProvider.transcribe`
- `LLMProvider.stream_generate`
- `TTSProvider.synthesize`

`ProviderRegistry` is the only place that maps configuration names to concrete
implementations. `StreamModule` receives ready provider instances and never imports an
OpenAI class.

## OpenAI Defaults

- STT sends a finalized WAV turn to `gpt-4o-transcribe`.
- LLM streams Responses API text deltas from `gpt-4.1-mini`.
- TTS streams 24 kHz PCM from `gpt-4o-mini-tts`.

The registry validates credentials before construction. Missing credentials raise a
clear startup/runtime error; there is no fake default.

## Adding Local Providers

To add Whisper, implement `STTProvider` and register a `local-whisper` constructor. To
add Ollama, implement `LLMProvider` with an async token iterator. To add Piper,
implement `TTSProvider` with an async PCM iterator.

Core queueing, events, tracing, WebSocket handling, Temporal signals, and persistence do
not change. Provider-specific model loading, process management, audio formats, and
timeouts remain inside the adapter.

Provider fallback configuration belongs in `provider_configs`. Temporal selects an
enabled alternative after `provider.failed`; the live runtime can later consume that
decision to swap the adapter for the next turn.

