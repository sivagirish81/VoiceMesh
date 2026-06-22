# ClickHouse Cloud Analytics

VoiceMesh can project coarse Kafka events into ClickHouse Cloud for historical,
cross-call analytics. Production deployments can also use Postgres CDC for
billing-facing analytics that must match committed ledger state. Both are analytics
side paths only:

```text
Session Worker -> Kafka -> ClickHouse Analytics Consumer -> ClickHouse Cloud -> Grafana
Postgres billing ledger -> CDC -> ClickHouse Cloud -> Grafana / BI
```

The live media path remains:

```text
Transport -> Session Worker -> VAD -> streaming STT -> streaming LLM -> streaming TTS -> Transport
```

ClickHouse is not used for VAD, barge-in, provider calls, queue backpressure,
billing correctness, webhook delivery, or Temporal workflow execution. If
ClickHouse Cloud is unavailable, calls continue and Kafka retains events for the
analytics consumer to retry later.

Postgres remains the billing source of truth. ClickHouse is where the team asks
cross-call and cross-tenant analytical questions over replicated facts.

## What ClickHouse Stores

The first integration stores coarse events in `voicemesh.voice_events`:

- call lifecycle events
- STT/LLM/TTS latency milestones
- usage events
- cork/uncork and stale chunk events
- barge-in candidate/confirmed/rejected events
- noise-like turns ignored
- provider failures
- billing/webhook/workflow completion events

It intentionally does not store raw audio, full transcripts, every LLM token,
every TTS chunk, every VAD frame, or trace spans. Jaeger remains the right tool
for a single-call trace. Prometheus remains the right tool for live operational
metrics.

For billing analytics, CDC can replicate selected Postgres tables such as
`usage_records`, `call_usage_manifests`, `call_usage_expectations`,
`final_call_billing_records`, `billing_adjustments`, tenant dimensions, and pricing
versions. PeerDB or ClickHouse ClickPipes for Postgres are good fits when the target is
ClickHouse Cloud.

## Manual Cloud Setup

1. Create or select a ClickHouse Cloud service.
2. Open the service's Connect screen and copy the HTTPS host and port.
3. Permit network access from the machine running VoiceMesh and Grafana.
4. Create a database named `voicemesh`, or let the bootstrap script create it.
5. Prefer separate users:
   - `voicemesh_writer`: `INSERT` and `SELECT` on `voicemesh.voice_events`
   - `voicemesh_grafana`: `SELECT` only on `voicemesh.voice_events`
6. Add credentials to local `.env`.
7. Never commit credentials.

If your Cloud account cannot create users or roles from SQL, create users in the
Cloud console and use the same table bootstrap.

## Environment

```env
CLICKHOUSE_ENABLED=true
CLICKHOUSE_HOST=<service-id>.<region>.<provider>.clickhouse.cloud
CLICKHOUSE_PORT=8443
CLICKHOUSE_SECURE=true
CLICKHOUSE_VERIFY_TLS=true
CLICKHOUSE_DATABASE=voicemesh
CLICKHOUSE_WRITER_USER=voicemesh_writer
CLICKHOUSE_WRITER_PASSWORD=<secret>
CLICKHOUSE_GRAFANA_USER=voicemesh_grafana
CLICKHOUSE_GRAFANA_PASSWORD=<secret>
CLICKHOUSE_BATCH_MAX_ROWS=500
CLICKHOUSE_BATCH_FLUSH_SECONDS=1
CLICKHOUSE_RETENTION_DAYS=30
```

TLS verification is enabled by default. Do not disable it for normal Cloud use.

## Bootstrap

The idempotent SQL template is:

```text
infra/clickhouse/cloud/bootstrap.sql.template
```

Run:

```bash
make clickhouse-cloud-check
make clickhouse-cloud-bootstrap
```

The bootstrap creates `voicemesh.voice_events` with a 30-day TTL by default.
User/role creation snippets are commented because Cloud permissions vary.

## Running The Consumer

The analytics consumer uses a dedicated Kafka consumer group:

```text
voicemesh-clickhouse-analytics
```

Start it locally:

```bash
make clickhouse-consumer
```

The consumer batches rows and commits Kafka offsets only after ClickHouse
acknowledges the insert. If the process crashes after ClickHouse accepts a batch
but before Kafka commit, duplicate delivery is acceptable. Rows use deterministic
`event_id` and the table uses `ReplacingMergeTree`, so dashboards should use
`uniqExact(event_id)` or unique call counts where exact deduplication matters.

This is at-least-once ingestion, not exactly-once delivery.

## Grafana

Grafana provisions:

- datasource: `VoiceMesh ClickHouse Cloud`
- datasource UID: `voicemesh-clickhouse-cloud`
- dashboard: `VoiceMesh Call Performance Analytics`
- dashboard: `VoiceMesh Reliability & Interaction Quality`

The Grafana datasource should use the read-only ClickHouse user. Passwords come
from environment variables and are not stored in dashboard JSON.

## Demo

```bash
make demo-clickhouse-cloud
```

The demo creates deterministic coarse events for:

- a healthy completed call
- slow TTS with cork/uncork
- noise and barge-in handling
- a provider failure

It then prints dashboard-relevant query results.

## Cost Guardrails

- coarse events only
- batched inserts
- no raw audio
- no full transcripts
- no token/audio-frame/VAD-frame events
- 30-day retention by default
- Grafana dashboards default to the last 24 hours
- dashboard refresh is 1 minute

Set `CLICKHOUSE_ENABLED=false` to stop ingestion. You can also stop or pause the
Cloud service after the demo credit period from the ClickHouse Cloud console.

## Failure Behavior

If ClickHouse is unavailable:

- active calls continue
- Kafka continues retaining events
- the ClickHouse consumer retries independently
- Grafana dashboards become stale
- Postgres remains the system of record and billing ledger
