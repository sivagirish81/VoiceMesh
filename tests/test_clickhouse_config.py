import pytest
from pydantic import ValidationError

from apps.api.config import Settings


def test_clickhouse_disabled_does_not_require_credentials() -> None:
    settings = Settings(_env_file=None, clickhouse_enabled=False)

    assert settings.clickhouse_enabled is False
    assert settings.clickhouse_secure is True
    assert settings.clickhouse_verify_tls is True


def test_clickhouse_enabled_requires_writer_credentials() -> None:
    with pytest.raises(ValidationError, match="CLICKHOUSE_HOST"):
        Settings(_env_file=None, clickhouse_enabled=True)


def test_clickhouse_batch_settings_are_validated() -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, clickhouse_batch_max_rows=0)
    with pytest.raises(ValidationError):
        Settings(_env_file=None, clickhouse_batch_flush_seconds=0)
