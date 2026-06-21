import json
from pathlib import Path


def test_postgres_publication_only_replicates_billing_tables() -> None:
    sql = Path("infra/postgres/billing_cdc_setup.sql.template").read_text()

    assert "call_usage_events" in sql
    assert "billing_line_items" in sql
    assert "final_call_billing_records" in sql
    assert "billing_adjustments" in sql
    assert "provider_configs" not in sql
    assert "webhook_deliveries" not in sql
    assert "call_events" not in sql


def test_clickhouse_cdc_bootstrap_creates_current_state_views() -> None:
    sql = Path("infra/clickhouse/cloud/cdc-bootstrap.sql.template").read_text()

    for view_name in (
        "billing_usage_current",
        "billing_line_items_current",
        "billing_calls_current",
        "billing_adjustments_current",
    ):
        assert f"CREATE OR REPLACE VIEW voicemesh.{view_name}" in sql
    assert "voicemesh_cdc.call_usage_events" in sql
    assert "voicemesh_cdc.billing_line_items" in sql


def test_billing_dashboards_use_clickhouse_datasource() -> None:
    dashboard_paths = [
        Path("infra/grafana/dashboards/voicemesh-cost-unit-economics.json"),
        Path("infra/grafana/dashboards/voicemesh-billing-integrity-cdc-health.json"),
    ]

    for path in dashboard_paths:
        dashboard = json.loads(path.read_text())
        assert dashboard["time"]["from"] == "now-24h"
        assert dashboard["refresh"] in {"30s", "1m"}
        for panel in dashboard["panels"]:
            assert panel["datasource"]["uid"] == "voicemesh-clickhouse-cloud"
