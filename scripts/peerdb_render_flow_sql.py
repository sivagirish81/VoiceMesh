import os
from pathlib import Path

TEMPLATE = Path("infra/peerdb/billing_mirror.flow.sql.template")
OUTPUT = Path("tmp/peerdb_billing_mirror.flow.sql")

REQUIRED = (
    "PEERDB_POSTGRES_PASSWORD",
    "CLICKHOUSE_HOST",
    "CLICKHOUSE_CDC_USER",
    "CLICKHOUSE_CDC_PASSWORD",
)

DEFAULTS = {
    "PEERDB_MIRROR_NAME": "voicemesh_billing_cdc",
    "PEERDB_SOURCE_PEER_NAME": "voicemesh_postgres",
    "PEERDB_DESTINATION_PEER_NAME": "voicemesh_clickhouse",
    "PEERDB_POSTGRES_HOST": "postgres",
    "PEERDB_POSTGRES_PORT": "5432",
    "PEERDB_POSTGRES_DATABASE": "voice_lab",
    "PEERDB_POSTGRES_USER": "voicemesh_peerdb",
    "CLICKHOUSE_PORT": "8443",
    "CLICKHOUSE_CDC_PORT": "9440",
}


def main() -> None:
    load_env_file()
    missing = [
        name for name in REQUIRED if not os.getenv(name) or os.getenv(name, "").startswith("<")
    ]
    if missing:
        raise SystemExit("Missing required environment variables: " + ", ".join(missing))
    rendered = TEMPLATE.read_text()
    for name, default in DEFAULTS.items():
        rendered = rendered.replace("{{" + name + "}}", os.getenv(name, default))
    for name in REQUIRED:
        rendered = rendered.replace("{{" + name + "}}", os.getenv(name, ""))
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(rendered)
    redacted = rendered
    for name in ("PEERDB_POSTGRES_PASSWORD", "CLICKHOUSE_CDC_PASSWORD"):
        value = os.getenv(name)
        if value:
            redacted = redacted.replace(value, "***")
    print(f"Rendered PeerDB Flow SQL: {OUTPUT}")
    print(redacted)


def load_env_file(path: str = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return
    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


if __name__ == "__main__":
    main()
