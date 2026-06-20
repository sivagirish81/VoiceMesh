# VoiceMesh ClickHouse Dashboard Queries

The provisioned ClickHouse dashboards use the raw `voicemesh.voice_events`
table directly. Queries are embedded in the dashboard JSON so Grafana can load
them without a separate provisioning step.

Design rules:

- Default time range is 24 hours.
- Refresh interval is 1 minute.
- Counts that represent calls use `uniqExact(call_id)`.
- Counts that represent events use `countIf(...)`.
- Latency panels use promoted `latency_ms` columns, not JSON parsing.
- The dashboards avoid transcripts, raw audio, LLM tokens, TTS chunks, and VAD
  frames.
- The ClickHouse datasource UID is `voicemesh-clickhouse-cloud`.
