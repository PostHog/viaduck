"""Tests for YAML config parsing, env var resolution, and validation."""

from __future__ import annotations

from pathlib import Path

import pytest

from viaduck.config import (
    ConfigError,
    PartitionConfig,
    RoutingConfig,
    _merge_defaults,
    _require_non_empty,
    _resolve_env_properties,
    _stable_hash,
    load,
)

MINIMAL_YAML = """\
source:
  name: src
  postgres_uri_env: SRC_PG
  data_path: s3://source/
  table: events

routing:
  field: company

destinations:
  - id: quacksworth-lake
    routing_value: quacksworth
    name: quacksworth_catalog
    postgres_uri_env: DEST_QW_PG
    data_path: s3://quacksworth/
"""


@pytest.fixture()
def config_file(tmp_path: Path) -> Path:
    p = tmp_path / "viaduck.yaml"
    p.write_text(MINIMAL_YAML)
    return p


@pytest.fixture(autouse=True)
def _set_env_vars(monkeypatch):
    monkeypatch.setenv("SRC_PG", "postgres://src")
    monkeypatch.setenv("DEST_QW_PG", "postgres://quacksworth")


# --- load() basics ---


def test_load_minimal(config_file: Path):
    cfg = load(config_file)
    assert cfg.source.name == "src"
    assert cfg.source.table == "events"
    assert cfg.routing.field == "company"
    assert len(cfg.destinations) == 1
    assert cfg.destinations[0].id == "quacksworth-lake"
    assert cfg.destinations[0].routing_value == "quacksworth"


def test_load_defaults_applied(config_file: Path):
    cfg = load(config_file)
    assert cfg.poll.interval_seconds == 5.0
    assert cfg.server.port == 8000
    assert cfg.web.enabled is True
    assert cfg.web.port == 8001
    assert cfg.instance.id == "viaduck-0"
    assert cfg.instance.partition.mode == "all"
    assert cfg.state.table == "_viaduck_state"


def test_load_missing_file():
    with pytest.raises(ConfigError, match="not found"):
        load("/nonexistent/path.yaml")


def test_load_missing_source(tmp_path: Path):
    p = tmp_path / "bad.yaml"
    p.write_text(
        "routing:\n  field: x\ndestinations:\n  - id: a\n    routing_value: a\n"
        "    name: a\n    postgres_uri_env: X\n    data_path: s3://a/\n"
    )
    with pytest.raises(ConfigError, match="source"):
        load(p)


def test_load_missing_routing(tmp_path: Path):
    p = tmp_path / "bad.yaml"
    p.write_text(
        "source:\n  name: s\n  postgres_uri_env: X\n  data_path: s3://s/\n  table: t\n"
        "destinations:\n  - id: a\n    routing_value: a\n    name: a\n    postgres_uri_env: X\n"
        "    data_path: s3://a/\n"
    )
    with pytest.raises(ConfigError, match="routing"):
        load(p)


def test_load_no_destinations(tmp_path: Path):
    p = tmp_path / "bad.yaml"
    p.write_text(
        "source:\n  name: s\n  postgres_uri_env: X\n  data_path: s3://s/\n  table: t\n"
        "routing:\n  field: x\ndestinations: []\n"
    )
    with pytest.raises(ConfigError, match="At least one destination"):
        load(p)


def test_load_duplicate_destination_ids(tmp_path: Path):
    p = tmp_path / "bad.yaml"
    content = MINIMAL_YAML + (
        "  - id: quacksworth-lake\n    routing_value: mallardine\n    name: b\n"
        "    postgres_uri_env: DEST_QW_PG\n    data_path: s3://b/\n"
    )
    p.write_text(content)
    with pytest.raises(ConfigError, match="Duplicate destination IDs"):
        load(p)


def test_load_duplicate_routing_values(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("D2_PG", "postgres://d2")
    p = tmp_path / "bad.yaml"
    content = MINIMAL_YAML + (
        "  - id: mallardine-lake\n    routing_value: quacksworth\n    name: b\n"
        "    postgres_uri_env: D2_PG\n    data_path: s3://b/\n"
    )
    p.write_text(content)
    with pytest.raises(ConfigError, match="Duplicate routing values"):
        load(p)


def test_destination_inherits_source_table(config_file: Path):
    cfg = load(config_file)
    assert cfg.destinations[0].table == "events"


def test_destination_custom_table(tmp_path: Path):
    p = tmp_path / "cfg.yaml"
    p.write_text(
        MINIMAL_YAML.replace(
            "    data_path: s3://quacksworth/", "    data_path: s3://quacksworth/\n    table: custom_events"
        )
    )
    cfg = load(p)
    assert cfg.destinations[0].table == "custom_events"


# --- Env var resolution ---


def test_resolve_env_properties(monkeypatch):
    monkeypatch.setenv("MY_SECRET", "s3cr3t")
    props = {"s3_endpoint": "minio:9000", "s3_access_key_id_env": "MY_SECRET"}
    resolved = _resolve_env_properties(props)
    assert resolved == {"s3_endpoint": "minio:9000", "s3_access_key_id": "s3cr3t"}


def test_resolve_env_properties_missing_var():
    props = {"key_env": "NONEXISTENT_VAR_XYZ"}
    with pytest.raises(ConfigError, match="NONEXISTENT_VAR_XYZ"):
        _resolve_env_properties(props)


def test_postgres_uri_resolved(config_file: Path):
    cfg = load(config_file)
    assert cfg.source.postgres_uri == "postgres://src"
    assert cfg.destinations[0].postgres_uri == "postgres://quacksworth"


def test_postgres_uri_missing_env(config_file: Path, monkeypatch):
    monkeypatch.delenv("SRC_PG")
    cfg = load(config_file)
    with pytest.raises(ConfigError, match="SRC_PG"):
        _ = cfg.source.postgres_uri


# --- Defaults merging ---


def test_merge_defaults():
    defaults = {"s3_endpoint": "minio:9000", "s3_use_ssl": "false"}
    dest = {"s3_endpoint": "custom:9000"}
    merged = _merge_defaults(dest, defaults)
    assert merged == {"s3_endpoint": "custom:9000", "s3_use_ssl": "false"}


def test_defaults_applied_to_destinations(tmp_path: Path):
    p = tmp_path / "cfg.yaml"
    p.write_text(MINIMAL_YAML + "\ndefaults:\n  properties:\n    s3_endpoint: minio:9000\n    s3_use_ssl: 'false'\n")
    cfg = load(p)
    assert cfg.destinations[0].properties["s3_endpoint"] == "minio:9000"


# --- Partition config ---


def test_partition_all(config_file: Path):
    cfg = load(config_file)
    assert cfg.assigned_destination_ids() == ["quacksworth-lake"]


def test_partition_explicit(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("D1_PG", "postgres://d1")
    monkeypatch.setenv("D2_PG", "postgres://d2")
    p = tmp_path / "cfg.yaml"
    content = """\
source:
  name: src
  postgres_uri_env: SRC_PG
  data_path: s3://source/
  table: events
routing:
  field: company
destinations:
  - id: a
    routing_value: acme
    name: a
    postgres_uri_env: D1_PG
    data_path: s3://a/
  - id: b
    routing_value: globex
    name: b
    postgres_uri_env: D2_PG
    data_path: s3://b/
instance:
  partition:
    mode: explicit
    include: [a]
"""
    p.write_text(content)
    cfg = load(p)
    assert cfg.assigned_destination_ids() == ["a"]


def test_partition_hash_deterministic():
    h1 = _stable_hash("team-123")
    h2 = _stable_hash("team-123")
    assert h1 == h2
    assert h1 != _stable_hash("team-456")


def test_partition_explicit_requires_include():
    with pytest.raises(ConfigError, match="include"):
        PartitionConfig(mode="explicit", include=[])


def test_partition_hash_invalid_ordinal():
    with pytest.raises(ConfigError, match="ordinal"):
        PartitionConfig(mode="hash", total=3, ordinal=5)


def test_partition_invalid_mode():
    with pytest.raises(ConfigError, match="mode"):
        PartitionConfig(mode="random")


# --- Pipeline name ---


def test_pipeline_name(config_file: Path):
    cfg = load(config_file)
    assert cfg.pipeline_name == "events-viaduck-0"


# --- destination_by_id ---


def test_destination_by_id(config_file: Path):
    cfg = load(config_file)
    d = cfg.destination_by_id("quacksworth-lake")
    assert d.name == "quacksworth_catalog"
    assert d.routing_value == "quacksworth"


def test_destination_by_id_unknown(config_file: Path):
    cfg = load(config_file)
    with pytest.raises(ConfigError, match="Unknown destination"):
        cfg.destination_by_id("nonexistent")


# --- Empty field validation (M3) ---


def test_require_non_empty_rejects_empty():
    with pytest.raises(ConfigError, match="non-empty"):
        _require_non_empty("", "test_field")


def test_require_non_empty_rejects_whitespace():
    with pytest.raises(ConfigError, match="non-empty"):
        _require_non_empty("   ", "test_field")


def test_require_non_empty_passes():
    assert _require_non_empty("value", "test_field") == "value"


def test_load_empty_source_name(tmp_path: Path):
    p = tmp_path / "bad.yaml"
    p.write_text(MINIMAL_YAML.replace("name: src", "name: ''"))
    with pytest.raises(ConfigError, match="source.name"):
        load(p)


def test_load_empty_destination_id(tmp_path: Path):
    p = tmp_path / "bad.yaml"
    p.write_text(MINIMAL_YAML.replace("id: quacksworth-lake", "id: ''"))
    with pytest.raises(ConfigError, match="destinations.*id"):
        load(p)


def test_load_empty_routing_field(tmp_path: Path):
    p = tmp_path / "bad.yaml"
    p.write_text(MINIMAL_YAML.replace("field: company", "field: ''"))
    with pytest.raises(ConfigError, match="routing.field"):
        load(p)


def test_load_missing_routing_value(tmp_path: Path):
    p = tmp_path / "bad.yaml"
    p.write_text(MINIMAL_YAML.replace("    routing_value: quacksworth\n", ""))
    with pytest.raises(ConfigError, match="routing_value"):
        load(p)


def test_load_integer_routing_value(tmp_path: Path):
    """YAML unquoted integer routing_value should be coerced to string (N2)."""
    p = tmp_path / "cfg.yaml"
    p.write_text(MINIMAL_YAML.replace("routing_value: quacksworth", "routing_value: 123"))
    cfg = load(p)
    assert cfg.destinations[0].routing_value == "123"
    assert isinstance(cfg.destinations[0].routing_value, str)


def test_load_unicode_routing_value(tmp_path: Path):
    """Unicode characters in routing_value should be accepted (N5)."""
    p = tmp_path / "cfg.yaml"
    p.write_text(MINIMAL_YAML.replace("routing_value: quacksworth", "routing_value: café"))
    cfg = load(p)
    assert cfg.destinations[0].routing_value == "café"


# --- Full config with all sections ---


def test_full_config(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("D_PG", "postgres://d")
    monkeypatch.setenv("S3_KEY", "mykey")
    p = tmp_path / "full.yaml"
    p.write_text("""\
source:
  name: src
  postgres_uri_env: SRC_PG
  data_path: s3://source/
  table: events
  properties:
    s3_access_key_id_env: S3_KEY

routing:
  field: company

destinations:
  - id: quacksworth-lake
    routing_value: quacksworth
    name: quacksworth_catalog
    postgres_uri_env: D_PG
    data_path: s3://quacksworth/
    table: events

poll:
  interval_seconds: 10

server:
  port: 9000

web:
  enabled: false
  port: 9001

state:
  table: _my_state

instance:
  id: prod-0
  partition:
    mode: all
""")
    cfg = load(p)
    assert cfg.poll.interval_seconds == 10.0
    assert cfg.server.port == 9000
    assert cfg.web.enabled is False
    assert cfg.web.port == 9001
    assert cfg.state.table == "_my_state"
    assert cfg.instance.id == "prod-0"
    assert cfg.source.resolved_properties() == {"s3_access_key_id": "mykey"}


# --- Edge: non-mapping YAML ---


def test_load_non_mapping_yaml(tmp_path: Path):
    p = tmp_path / "bad.yaml"
    p.write_text("- a list\n- not a mapping\n")
    with pytest.raises(ConfigError, match="YAML mapping"):
        load(p)


# --- key_columns config ---


def test_load_with_key_columns(tmp_path: Path):
    """YAML with key_columns: [event_id, company] parses correctly."""
    p = tmp_path / "cfg.yaml"
    p.write_text(MINIMAL_YAML.replace("  field: company", "  field: company\n  key_columns: [event_id, company]"))
    cfg = load(p)
    assert cfg.routing.key_columns == ["event_id", "company"]


def test_load_without_key_columns_defaults_empty(config_file: Path):
    """YAML without key_columns defaults to []."""
    cfg = load(config_file)
    assert cfg.routing.key_columns == []


def test_load_key_columns_empty_list(tmp_path: Path):
    """Explicit key_columns: [] is valid."""
    p = tmp_path / "cfg.yaml"
    p.write_text(MINIMAL_YAML.replace("  field: company", "  field: company\n  key_columns: []"))
    cfg = load(p)
    assert cfg.routing.key_columns == []


def test_routing_config_has_key_columns():
    """RoutingConfig dataclass has key_columns field."""
    rc = RoutingConfig(field="company", key_columns=["event_id", "company"])
    assert rc.key_columns == ["event_id", "company"]

    rc_default = RoutingConfig(field="company")
    assert rc_default.key_columns == []


# --- seed_mode config ---


def test_seed_mode_default_is_scan():
    """RoutingConfig defaults seed_mode to 'scan'."""
    rc = RoutingConfig(field="company")
    assert rc.seed_mode == "scan"


def test_seed_mode_cdc_replay():
    """seed_mode='cdc_replay' is valid."""
    rc = RoutingConfig(field="company", seed_mode="cdc_replay")
    assert rc.seed_mode == "cdc_replay"


def test_seed_mode_invalid():
    """seed_mode='bogus' raises ConfigError."""
    with pytest.raises(ConfigError, match="seed_mode"):
        RoutingConfig(field="company", seed_mode="bogus")


def test_load_with_seed_mode(tmp_path: Path):
    """YAML with seed_mode parses correctly."""
    p = tmp_path / "cfg.yaml"
    p.write_text(MINIMAL_YAML.replace("  field: company", "  field: company\n  seed_mode: cdc_replay"))
    cfg = load(p)
    assert cfg.routing.seed_mode == "cdc_replay"
