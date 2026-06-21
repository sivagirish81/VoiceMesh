# PeerDB CDC To ClickHouse Cloud

VoiceMesh can demonstrate a billing analytics pattern where Postgres remains the
authoritative billing ledger and ClickHouse Cloud receives only committed billing state
through CDC:

```text
Kafka usage event
-> Postgres UsageWriter
-> committed billing tables
-> Postgres WAL
-> local PeerDB
-> ClickHouse Cloud
-> Grafana
```

This path is outside the live voice loop. PeerDB, ClickHouse, and Grafana do not control
VAD, STT, LLM, TTS, transport playback, barge-in, Temporal workflow decisions, or
Postgres billing commits.

## Replicated Tables

The local publication is `voicemesh_billing_publication` and includes only:

- `call_usage_events`
- `billing_line_items`
- `final_call_billing_records`
- `billing_adjustments`

It intentionally excludes provider config, webhook secrets, transcripts, raw audio,
tool arguments, call events, and authentication data.

## Local Setup

Postgres runs with logical replication enabled:

```text
wal_level=logical
max_replication_slots=10
max_wal_senders=10
max_slot_wal_keep_size=1GB
```

`max_slot_wal_keep_size` protects local disk space. If PeerDB is stopped long enough for
the slot to exceed that bound, the slot can become invalid and the mirror may require a
resnapshot.

If you already had the Docker stack running before enabling CDC, recreate or restart the
Postgres service with the updated Compose command and then verify with:

```bash
make peerdb-status
```

The status output should show `wal_level: logical`.

## Environment

Add local-only values to `.env`:

```env
PEERDB_ENABLED=true
PEERDB_POSTGRES_PASSWORD=<local-secret>
PEERDB_SQL_PASSWORD=peerdb
CLICKHOUSE_CDC_USER=voicemesh_cdc
CLICKHOUSE_CDC_PASSWORD=<cloud-secret>
CLICKHOUSE_CDC_PORT=9440
PEERDB_CLICKHOUSE_AWS_CREDENTIALS_AWS_ACCESS_KEY_ID=<cloud-object-store-access-key>
PEERDB_CLICKHOUSE_AWS_CREDENTIALS_AWS_SECRET_ACCESS_KEY=<cloud-object-store-secret-key>
PEERDB_CLICKHOUSE_AWS_CREDENTIALS_AWS_REGION=us-east-1
PEERDB_CLICKHOUSE_AWS_CREDENTIALS_AWS_ENDPOINT_URL_S3=https://s3.amazonaws.com
PEERDB_CLICKHOUSE_AWS_S3_BUCKET_NAME=<cloud-reachable-peerdb-stage-bucket>
CLICKHOUSE_GRAFANA_USER=voicemesh_grafana
CLICKHOUSE_GRAFANA_PASSWORD=<cloud-secret>
```

Use separate ClickHouse users for CDC writes and Grafana reads. The Grafana user should
be read-only.

## Commands

```bash
make up-cdc
docker compose --profile cdc -f docker-compose.yml -f docker-compose.peerdb.yml ps peerdb
make peerdb-postgres-setup
make clickhouse-cdc-bootstrap
make peerdb-create-mirror
make peerdb-status
make demo-billing-cdc
```

`make up-cdc` starts a local PeerDB deployment under the `cdc` Compose profile. It is
not one container; it includes PeerDB's catalog Postgres, PeerDB's own Temporal service,
flow API/workers, MinIO staging, the PeerDB SQL endpoint, and the PeerDB UI. The useful
local ports are:

- PeerDB SQL endpoint: `localhost:9900`
- PeerDB UI: `http://localhost:9902`
- PeerDB flow API: `localhost:8112` / `localhost:8113`
- PeerDB MinIO: `localhost:9001` / console `localhost:9002`

`peerdb-create-mirror` renders `tmp/peerdb_billing_mirror.flow.sql` from `.env` and
applies it through PeerDB's Postgres-compatible SQL endpoint:

```bash
psql "port=9900 host=localhost password=${PEERDB_SQL_PASSWORD:-peerdb}" \
  -f tmp/peerdb_billing_mirror.flow.sql
```

`tmp/` is ignored by Git because the rendered file contains local connection secrets.

Important ClickHouse Cloud staging note: PeerDB stages data in object storage before
loading ClickHouse. The bundled local MinIO works only if the destination ClickHouse
service can reach the configured MinIO endpoint. For a remote ClickHouse Cloud service,
that usually means replacing the local MinIO endpoint with a cloud-reachable S3/GCS
bucket or using the managed ClickHouse/PeerDB integration. Without that, peers may be
created but the mirror can fail during initial copy/load because ClickHouse Cloud cannot
fetch staged files from your laptop.

Also note that PeerDB uses the ClickHouse native protocol for the destination peer. The
ClickHouse Cloud HTTPS/curl port is commonly `8443`, but PeerDB needs the secure native
port, commonly `9440`.

If you see a PeerDB flow error like:

```text
Not found address of host: host.docker.internal
```

the mirror is working far enough to stage files, but ClickHouse Cloud cannot fetch those
files from local MinIO. Configure the `PEERDB_CLICKHOUSE_AWS_*` variables to point at a
cloud-reachable bucket, restart `make up-cdc`, and recreate/resnapshot the mirror.

## ClickHouse Layout

PeerDB writes raw committed-state tables under:

```text
voicemesh_cdc
```

Grafana queries stable analytical views under:

```text
voicemesh.billing_usage_current
voicemesh.billing_line_items_current
voicemesh.billing_calls_current
voicemesh.billing_adjustments_current
```

The dashboards do not query raw CDC tables directly.

## Dashboards

Grafana provisions:

- `VoiceMesh Cost & Unit Economics`
- `VoiceMesh Billing Integrity & CDC Health`

The dashboards use micro-unit monetary values and convert to USD in queries.

## Failure Semantics

If PeerDB or ClickHouse is unavailable:

- live calls continue;
- Kafka keeps usage events;
- Postgres billing writes continue;
- Temporal billing workflows continue;
- Grafana becomes stale;
- the CDC slot retains WAL until PeerDB catches up or the local retention bound is hit.

Postgres remains authoritative. ClickHouse is eventually consistent and analytical.

## Production Direction

The local lab is intentionally small:

- one local Postgres container;
- one local PeerDB deployment;
- one ClickHouse Cloud service;
- local Grafana.

Production hardening would add managed or highly available Postgres, failover-aware
replication slots, managed ClickPipes or replicated PeerDB workers, CDC lag alerting,
WAL-retention safeguards, schema-change controls, reconciliation jobs, and operational
runbooks for resnapshot/recovery.

## POC Limitation

Some PeerDB-to-ClickHouse load paths use a staging object store such as MinIO. When the
destination is ClickHouse Cloud, the Cloud service may need network access to that
staging endpoint. A laptop-local MinIO endpoint is not automatically reachable from
ClickHouse Cloud. If your PeerDB version requires staging, expose/configure the staging
endpoint deliberately or use a reachable object store. Do not expose local Postgres
publicly.
