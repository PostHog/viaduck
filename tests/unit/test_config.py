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
    _validate_partition_by,
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
  mode: append_only

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
    monkeypatch.setenv("SRC_PG", "postgres:host=src")
    monkeypatch.setenv("DEST_QW_PG", "postgres:host=quacksworth")


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
    # 0 = auto-derive per-destination cap as buffer_total_max_bytes / N at
    # DeliveryManager init; explicit values pass through (validated >= 0).
    assert cfg.delivery.buffer_max_bytes_per_destination == 0
    assert cfg.web.enabled is True
    assert cfg.instance.id == "viaduck-0"
    assert cfg.instance.partition.mode == "all"
    assert cfg.state.table == "viaduck_state"
    assert cfg.state.postgres_uri_env is None


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
        "routing:\n  field: x\n  mode: append_only\ndestinations: []\n"
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
    monkeypatch.setenv("D2_PG", "postgres:host=d2")
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


def test_routing_mode_required(config_file: Path):
    """Loading the MINIMAL_YAML (which sets mode: append_only) parses fine —
    this is the baseline the matrix tests below mutate."""
    cfg = load(config_file)
    assert cfg.routing.mode == "append_only"


def test_routing_mode_unset_rejected(tmp_path: Path):
    """No mode → ConfigError. We don't want to fall back to inferring from
    key_columns presence; that was the old silent-misconfig hazard.

    A missing key resolves to the dataclass default `mode=""`, which the
    empty-case branch matches with a "required, no default" message."""
    raw = MINIMAL_YAML.replace("\n  mode: append_only", "")
    p = tmp_path / "no_mode.yaml"
    p.write_text(raw)
    with pytest.raises(ConfigError, match=r"routing.mode is required, no default"):
        load(p)


def test_routing_mode_unknown_value_rejected(tmp_path: Path):
    """Typos or stale legacy values fail loudly with the enum listed in the
    error so the operator can self-correct without grepping source."""
    raw = MINIMAL_YAML.replace("mode: append_only", "mode: cdc_replay")
    p = tmp_path / "bad_mode.yaml"
    p.write_text(raw)
    with pytest.raises(ConfigError, match=r"routing.mode must be one of \['full_cdc', 'append_only'\]"):
        load(p)


def test_routing_mode_full_cdc_requires_key_columns(tmp_path: Path):
    """The whole point of full_cdc is the upsert join keys — empty
    key_columns is operator misconfig (the old derivation flipped silently
    to append_only here)."""
    raw = MINIMAL_YAML.replace("mode: append_only", "mode: full_cdc")
    p = tmp_path / "full_cdc_no_keys.yaml"
    p.write_text(raw)
    with pytest.raises(ConfigError, match="full_cdc.*requires.*key_columns"):
        load(p)


def test_routing_mode_full_cdc_with_key_columns_ok(tmp_path: Path):
    raw = MINIMAL_YAML.replace("mode: append_only", "mode: full_cdc\n  key_columns: [uuid]")
    p = tmp_path / "full_cdc_with_keys.yaml"
    p.write_text(raw)
    cfg = load(p)
    assert cfg.routing.mode == "full_cdc"
    assert cfg.routing.key_columns == ["uuid"]


def test_routing_mode_yaml_bool_coerced_rejected(tmp_path: Path):
    """YAML 1.1 coerces `mode: yes` to Python True (PyYAML behavior). The
    validator catches the type mismatch with a quote-hint before the enum
    check fires with a confusing `got True` message."""
    raw = MINIMAL_YAML.replace("mode: append_only", "mode: yes")
    p = tmp_path / "yaml_bool.yaml"
    p.write_text(raw)
    with pytest.raises(ConfigError, match=r"routing.mode must be a string"):
        load(p)


def test_routing_mode_empty_string_distinguished_from_unknown(tmp_path: Path):
    """`mode:` (empty value) is operator-omission, not a typo. Distinct
    error message tells them to set it rather than guess the typo."""
    raw = MINIMAL_YAML.replace("mode: append_only", "mode: ''")
    p = tmp_path / "empty_mode.yaml"
    p.write_text(raw)
    with pytest.raises(ConfigError, match=r"routing.mode is required, no default"):
        load(p)


def test_routing_mode_null_value_routes_to_required_message(tmp_path: Path):
    """`mode:` (key present, no value) parses to Python None via PyYAML.
    The loader coerces None→"" before construction so the operator gets the
    "is required" guidance instead of the isinstance "quote the value if
    YAML coerced it" hint — which would point them at the wrong fix."""
    raw = MINIMAL_YAML.replace("mode: append_only", "mode:")
    p = tmp_path / "null_mode.yaml"
    p.write_text(raw)
    with pytest.raises(ConfigError, match=r"routing.mode is required, no default"):
        load(p)


def test_drop_source_columns_without_projection_flag_rejected(tmp_path: Path):
    """`drop_source_columns` is a no-op if `schema_projection_enabled=false` —
    the write path skips projection entirely. Reject at load rather than
    silently ignore the drop and let positional-insert scramble the target."""
    raw = MINIMAL_YAML + "    drop_source_columns: [captured_at]\n"
    p = tmp_path / "drop_without_flag.yaml"
    p.write_text(raw)
    with pytest.raises(ConfigError, match="drop_source_columns.*schema_projection_enabled=false"):
        load(p)


def test_drop_source_columns_with_projection_flag_ok(tmp_path: Path):
    """Same drop is legal when the flag is on."""
    raw = MINIMAL_YAML + "    schema_projection_enabled: true\n    drop_source_columns: [captured_at]\n"
    p = tmp_path / "drop_with_flag.yaml"
    p.write_text(raw)
    cfg = load(p)
    assert cfg.destinations[0].schema_projection_enabled
    assert cfg.destinations[0].drop_source_columns == ("captured_at",)


def test_buffer_max_bytes_per_destination_negative_rejected():
    from viaduck.config import DeliveryConfig

    with pytest.raises(ConfigError, match="buffer_max_bytes_per_destination"):
        DeliveryConfig(buffer_max_bytes_per_destination=-1)


def test_buffer_max_bytes_per_destination_loader_passthrough(tmp_path: Path):
    raw = MINIMAL_YAML + "\ndelivery:\n  buffer_max_bytes_per_destination: 33554432\n"
    p = tmp_path / "per_dest_cap.yaml"
    p.write_text(raw)
    cfg = load(p)
    assert cfg.delivery.buffer_max_bytes_per_destination == 33554432


def test_buffer_max_bytes_per_destination_quoted_string_rejected(tmp_path: Path):
    """YAML `\"33554432\"` (quoted) must be rejected by _validate_int, not
    silently coerced — the same foot-gun guard every other delivery int has."""
    raw = MINIMAL_YAML + '\ndelivery:\n  buffer_max_bytes_per_destination: "33554432"\n'
    p = tmp_path / "per_dest_cap_str.yaml"
    p.write_text(raw)
    with pytest.raises(ConfigError, match="buffer_max_bytes_per_destination must be an integer"):
        load(p)


def test_routing_mode_append_only_forbids_key_columns(tmp_path: Path):
    """append_only's apply path doesn't use key_columns at all; a non-empty
    list is misconfig the operator must clear or switch modes for."""
    raw = MINIMAL_YAML.replace("mode: append_only", "mode: append_only\n  key_columns: [uuid]")
    p = tmp_path / "append_only_with_keys.yaml"
    p.write_text(raw)
    with pytest.raises(ConfigError, match="append_only.*forbids.*key_columns"):
        load(p)


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
    assert cfg.source.postgres_uri == "postgres:host=src"
    assert cfg.destinations[0].postgres_uri == "postgres:host=quacksworth"


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
    monkeypatch.setenv("D1_PG", "postgres:host=d1")
    monkeypatch.setenv("D2_PG", "postgres:host=d2")
    p = tmp_path / "cfg.yaml"
    content = """\
source:
  name: src
  postgres_uri_env: SRC_PG
  data_path: s3://source/
  table: events
routing:
  field: company
  mode: append_only
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
    monkeypatch.setenv("D_PG", "postgres:host=d")
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
  mode: append_only

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
    """YAML with key_columns: [event_id, company] parses correctly under full_cdc."""
    p = tmp_path / "cfg.yaml"
    p.write_text(MINIMAL_YAML.replace("mode: append_only", "mode: full_cdc\n  key_columns: [event_id, company]"))
    cfg = load(p)
    assert cfg.routing.key_columns == ["event_id", "company"]


def test_load_without_key_columns_defaults_empty(config_file: Path):
    """YAML without key_columns defaults to []. The MINIMAL_YAML config is
    mode: append_only, so this implicitly also asserts append_only doesn't
    require the field."""
    cfg = load(config_file)
    assert cfg.routing.key_columns == []


def test_routing_config_has_key_columns():
    """RoutingConfig dataclass has key_columns field."""
    rc = RoutingConfig(field="company", mode="full_cdc", key_columns=["event_id", "company"])
    assert rc.key_columns == ["event_id", "company"]

    rc_default = RoutingConfig(field="company", mode="append_only")
    assert rc_default.key_columns == []


# --- seed_mode config ---


def test_seed_mode_default_is_scan():
    """RoutingConfig defaults seed_mode to 'scan'."""
    rc = RoutingConfig(field="company", mode="append_only")
    assert rc.seed_mode == "scan"


def test_seed_mode_earliest():
    rc = RoutingConfig(field="company", mode="append_only", seed_mode="earliest")
    assert rc.seed_mode == "earliest"


def test_seed_mode_latest():
    rc = RoutingConfig(field="company", mode="append_only", seed_mode="latest")
    assert rc.seed_mode == "latest"


def test_seed_mode_invalid():
    """seed_mode='bogus' raises ConfigError."""
    with pytest.raises(ConfigError, match="seed_mode"):
        RoutingConfig(field="company", mode="append_only", seed_mode="bogus")


def test_seed_mode_cdc_replay_invalid():
    """cdc_replay is no longer valid; use earliest or latest."""
    with pytest.raises(ConfigError, match="seed_mode"):
        RoutingConfig(field="company", mode="append_only", seed_mode="cdc_replay")


def test_load_with_seed_mode(tmp_path: Path):
    """YAML with seed_mode=latest parses correctly."""
    p = tmp_path / "cfg.yaml"
    p.write_text(MINIMAL_YAML.replace("  field: company", "  field: company\n  seed_mode: latest"))
    cfg = load(p)
    assert cfg.routing.seed_mode == "latest"


def test_state_postgres_uri_defaults_to_source(config_file, monkeypatch):
    """state.postgres_uri_env unset → the cursor store shares the source
    catalog's Postgres."""
    monkeypatch.setenv("SRC_PG", "postgresql://src:5432/meta")
    cfg = load(config_file)
    assert cfg.state.resolve_postgres_uri(cfg.source) == "postgresql://src:5432/meta"


def test_state_postgres_uri_explicit_override(config_file, monkeypatch, tmp_path):
    import yaml as _yaml

    raw = _yaml.safe_load(config_file.read_text())
    raw["state"] = {"postgres_uri_env": "STATE_PG_URI"}
    override = tmp_path / "viaduck-state-override.yaml"
    override.write_text(_yaml.dump(raw))
    monkeypatch.setenv("STATE_PG_URI", "postgresql://state-host:5432/cursors")
    cfg = load(override)
    assert cfg.state.resolve_postgres_uri(cfg.source) == "postgresql://state-host:5432/cursors"


def test_state_uri_translates_duckdb_attach_format(config_file, monkeypatch):
    """The source URI uses DuckDB's ATTACH format ('postgres:' + libpq
    keyword/value). The state store must receive valid libpq conninfo."""
    monkeypatch.setenv("SRC_PG", "postgres:host=postgres-source port=5432 dbname=meta user=v password=x")
    cfg = load(config_file)
    assert cfg.state.resolve_postgres_uri(cfg.source) == (
        "host=postgres-source port=5432 dbname=meta user=v password=x"
    )


def test_state_uri_passes_through_libpq_forms(config_file, monkeypatch):
    for raw in (
        "postgresql://u:p@h:5432/db",
        "postgres://u:p@h:5432/db",
        "host=h port=5432 dbname=db",
    ):
        monkeypatch.setenv("SRC_PG", raw)
        cfg = load(config_file)
        assert cfg.state.resolve_postgres_uri(cfg.source) == raw


# --- log_summary ---


def _captured_log_lines(cfg, caplog) -> list[str]:
    """Helper: call cfg.log_summary and return the rendered messages.

    log_summary uses lazy % formatting; the rendered text is on record.message,
    not record.msg. Filter to the config-summary records by the "config: " prefix
    so a stray unrelated log doesn't pollute the assertion set."""
    import logging as _logging

    log = _logging.getLogger("viaduck.test.config_summary")
    with caplog.at_level(_logging.INFO, logger="viaduck.test.config_summary"):
        cfg.log_summary(log)
    return [r.getMessage() for r in caplog.records if r.getMessage().startswith("config: ")]


def test_log_summary_covers_every_top_level_section(config_file, caplog):
    """Every top-level config section dumps at least one line. Catches the
    case where a new section is added to ViaduckConfig but log_summary
    isn't extended — the deploy log would silently miss the new values."""
    cfg = load(config_file)
    lines = _captured_log_lines(cfg, caplog)

    # Section coverage: at least one line whose key starts with each section name.
    section_prefixes = [
        "source.",
        "routing.",
        "poll.",
        "delivery.",
        "server.",
        "web.",
        "instance.",
        "state.",
        "destinations",  # both destinations.count and destinations[i].*
    ]
    for prefix in section_prefixes:
        assert any(f"config: {prefix}" in line for line in lines), (
            f"no log line for section {prefix!r}; new field added without updating log_summary?"
        )


def test_log_summary_one_line_per_field_not_grouped(config_file, caplog):
    """Each leaf field gets its own log line so operators can grep for
    individual values (the whole point of the "one line per config" shape).
    Defends against a refactor that bundles multiple fields onto one line."""
    cfg = load(config_file)
    lines = _captured_log_lines(cfg, caplog)

    # Spot-check known leaf fields each appear on their own line.
    for needle in [
        "config: source.name=",
        "config: source.table=",
        "config: routing.field=",
        "config: routing.seed_mode=",
        "config: delivery.workers=",
        "config: delivery.flush_interval_seconds=",
        "config: delivery.flush_batch_max_rows=",
        "config: delivery.flush_deadline_seconds=",
        "config: delivery.flush_circuit_failures=",
        "config: poll.interval_seconds=",
        "config: poll.read_unit max_rows=",
        "config: instance.partition.mode=",
        "config: state.table=",
        "config: state.schema=",
    ]:
        matching = [line for line in lines if line.startswith(needle)]
        assert len(matching) == 1, f"expected exactly one line for {needle!r}, got {matching!r}"


def test_log_summary_destination_fields_per_index(config_file, caplog):
    """Each destination's fields appear under destinations[i].* so multi-
    destination configs stay disambiguated by index (not name) in the log."""
    cfg = load(config_file)
    lines = _captured_log_lines(cfg, caplog)

    assert "config: destinations.count=1" in lines
    assert any("config: destinations[0].id='quacksworth-lake'" == line for line in lines)
    assert any("config: destinations[0].routing_value='quacksworth'" == line for line in lines)


def test_log_summary_routing_mode_logged(config_file, caplog):
    """The new routing.mode field must show up in the per-leaf log dump so
    operators can grep `config: routing.mode=` to confirm what the live pod
    is actually configured for. Defends against a future drop-from-log
    refactor that hides the field that determines the entire pipeline shape."""
    cfg = load(config_file)
    lines = _captured_log_lines(cfg, caplog)
    assert any("config: routing.mode='append_only'" == line for line in lines)


def test_log_summary_does_not_log_resolved_postgres_uri(config_file, caplog, monkeypatch):
    """Env-var-resolved credentials (DB passwords inside the URI, S3 keys) must
    never appear in the log — only env var NAMES. Catches a future refactor
    that calls .postgres_uri (the resolved @property) instead of the raw field."""
    monkeypatch.setenv("SRC_PG", "postgres:host=src password=SUPER_SECRET_PG_PW")
    monkeypatch.setenv("DEST_QW_PG", "postgres:host=qw password=SUPER_SECRET_DEST_PW")
    cfg = load(config_file)
    lines = _captured_log_lines(cfg, caplog)

    joined = "\n".join(lines)
    assert "SUPER_SECRET_PG_PW" not in joined, "source resolved postgres URI leaked into the log"
    assert "SUPER_SECRET_DEST_PW" not in joined, "destination resolved postgres URI leaked into the log"
    # Env var NAMES are safe and SHOULD appear.
    assert any("config: source.postgres_uri_env='SRC_PG'" == line for line in lines)
    assert any("config: destinations[0].postgres_uri_env='DEST_QW_PG'" == line for line in lines)


def test_log_summary_does_not_log_resolved_s3_credentials(tmp_path, caplog, monkeypatch):
    """Properties dicts hold env var NAMES (s3_access_key_id_env: MY_KEY_ENV),
    never the resolved credentials. The resolved_properties() @property
    resolves env values; log_summary must dump the raw properties dict only."""
    p = tmp_path / "cfg.yaml"
    p.write_text(
        MINIMAL_YAML
        + "\ndefaults:\n  properties:\n    s3_access_key_id_env: 'S3_KEY'\n    s3_secret_access_key_env: 'S3_SECRET'\n"
    )
    monkeypatch.setenv("S3_KEY", "AKIA_RESOLVED_KEY_ID_SHOULD_NOT_APPEAR")
    monkeypatch.setenv("S3_SECRET", "RESOLVED_S3_SECRET_SHOULD_NOT_APPEAR")
    cfg = load(p)
    lines = _captured_log_lines(cfg, caplog)

    joined = "\n".join(lines)
    assert "AKIA_RESOLVED_KEY_ID_SHOULD_NOT_APPEAR" not in joined
    assert "RESOLVED_S3_SECRET_SHOULD_NOT_APPEAR" not in joined
    # The env-var names themselves are fine — they're just identifiers.
    assert any("s3_access_key_id_env" in line and "S3_KEY" in line for line in lines)


# ----------------------------------------------------------------------
# partition_by parsing
# ----------------------------------------------------------------------


def test_validate_partition_by_none_returns_empty_tuple():
    assert _validate_partition_by(None, "destinations[0].partition_by") == ()


def test_validate_partition_by_empty_list_returns_empty_tuple():
    assert _validate_partition_by([], "destinations[0].partition_by") == ()


def test_validate_partition_by_identity_columns():
    result = _validate_partition_by(["team_id", "shard"], "ctx")
    assert result == (("", "team_id"), ("", "shard"))


def test_validate_partition_by_transforms():
    result = _validate_partition_by(
        ["year(_inserted_at)", "month(_inserted_at)", "day(_inserted_at)", "hour(_inserted_at)"],
        "ctx",
    )
    assert result == (
        ("year", "_inserted_at"),
        ("month", "_inserted_at"),
        ("day", "_inserted_at"),
        ("hour", "_inserted_at"),
    )


def test_validate_partition_by_mixed_identity_and_transforms():
    result = _validate_partition_by(
        ["team_id", "year(_inserted_at)", "month(_inserted_at)"],
        "ctx",
    )
    assert result == (("", "team_id"), ("year", "_inserted_at"), ("month", "_inserted_at"))


def test_validate_partition_by_case_insensitive_transform_name():
    """Mixed casing on the function name (YEAR, Year) is normalized to lowercase."""
    result = _validate_partition_by(["YEAR(ts)", "Month(ts)"], "ctx")
    assert result == (("year", "ts"), ("month", "ts"))


def test_validate_partition_by_lowercases_column_identifiers():
    """Mixed-case column identifiers are normalized to lowercase so the
    downstream pyducklake quoting (`"X"`) matches DuckDB's case-folded
    storage of unquoted identifiers. Regression for the SWE-review H4
    landmine — a config of `year(Inserted_At)` would have failed at
    ALTER time because the actual column is `inserted_at` (case-folded
    at table creation).
    """
    result = _validate_partition_by(
        ["Team_Id", "Year(Inserted_At)", "Month(InsertedAt)"],
        "ctx",
    )
    assert result == (
        ("", "team_id"),
        ("year", "inserted_at"),
        ("month", "insertedat"),
    )


def test_validate_partition_by_unknown_transform_rejected():
    with pytest.raises(ConfigError, match="unknown transform 'bucket'"):
        _validate_partition_by(["bucket(id)"], "ctx")


def test_validate_partition_by_non_list_rejected():
    with pytest.raises(ConfigError, match="must be a list"):
        _validate_partition_by("year(ts)", "ctx")


def test_validate_partition_by_non_string_entry_rejected():
    with pytest.raises(ConfigError, match=r"\[0\] must be a string"):
        _validate_partition_by([42], "ctx")


def test_validate_partition_by_empty_string_entry_rejected():
    with pytest.raises(ConfigError, match=r"\[1\] is empty"):
        _validate_partition_by(["team_id", ""], "ctx")


def test_validate_partition_by_malformed_parens_rejected():
    with pytest.raises(ConfigError, match="must be 'col' or 'func"):
        _validate_partition_by(["year(ts"], "ctx")


def test_validate_partition_by_invalid_identifier_rejected():
    with pytest.raises(ConfigError, match="not a valid column identifier"):
        _validate_partition_by(["team-id"], "ctx")  # hyphen, not allowed


def test_load_destination_partition_by_from_yaml(tmp_path: Path):
    """YAML round-trip — destinations[i].partition_by parses into DestinationConfig.partition_by."""
    p = tmp_path / "cfg.yaml"
    p.write_text(
        MINIMAL_YAML
        + """
destinations:
  - id: team-2
    routing_value: "2"
    name: team-2
    postgres_uri_env: DEST_PG
    data_path: s3://dest/
    table: posthog.events_nrt
    partition_by:
      - team_id
      - year(_inserted_at)
      - month(_inserted_at)
"""
    )
    cfg = load(p)
    assert cfg.destinations[0].partition_by == (
        ("", "team_id"),
        ("year", "_inserted_at"),
        ("month", "_inserted_at"),
    )


def test_load_destination_partition_by_defaults_to_empty(config_file):
    """Destinations without partition_by parse to empty tuple — backwards compatible."""
    cfg = load(config_file)
    for dest in cfg.destinations:
        assert dest.partition_by == ()


def test_discovery_secret_cache_ttl_loads_from_yaml(tmp_path):
    # Review F1: a YAML key the loader ignores is a silent misconfig —
    # the validation in __post_init__ would be dead code for the YAML
    # path without this wiring.
    import yaml

    from viaduck.config import load

    cfg_yaml = {
        "source": {"name": "s", "postgres_uri_env": "SRC", "data_path": "/d", "table": "t"},
        "routing": {"field": "company", "mode": "append_only"},
        "destinations": [
            {
                "id": "d1",
                "routing_value": "a",
                "name": "n",
                "postgres_uri_env": "DST",
                "data_path": "/d",
                "table": "t",
            }
        ],
        "discovery": {"secret_cache_ttl_s": 600},
    }
    p = tmp_path / "viaduck.yaml"
    p.write_text(yaml.safe_dump(cfg_yaml))
    cfg = load(str(p))
    assert cfg.discovery.secret_cache_ttl_s == 600.0


def test_flush_adaptive_defaults_enabled():
    from viaduck.config import DeliveryConfig

    cfg = DeliveryConfig()
    assert cfg.flush_adaptive is True
    assert cfg.flush_adaptive_low_seconds == 5.0
    assert cfg.flush_adaptive_high_seconds == 30.0
    assert cfg.flush_adaptive_step_rows == 4_000
    assert cfg.flush_adaptive_min_rows == 4_000
    assert cfg.flush_adaptive_reprobe_after == 50


def test_flush_adaptive_band_must_be_ordered():
    from viaduck.config import DeliveryConfig

    with pytest.raises(ConfigError, match="flush_adaptive_high_seconds"):
        DeliveryConfig(flush_adaptive_low_seconds=30.0, flush_adaptive_high_seconds=5.0)


def test_flush_adaptive_loader_passthrough(tmp_path: Path):
    raw = MINIMAL_YAML + (
        "\ndelivery:\n"
        "  flush_adaptive: false\n"
        "  flush_adaptive_low_seconds: 2.5\n"
        "  flush_adaptive_high_seconds: 60\n"
        "  flush_adaptive_step_rows: 8000\n"
        "  flush_adaptive_min_rows: 2000\n"
        "  flush_adaptive_reprobe_after: 25\n"
    )
    p = tmp_path / "adaptive.yaml"
    p.write_text(raw)
    cfg = load(p)
    assert cfg.delivery.flush_adaptive is False
    assert cfg.delivery.flush_adaptive_low_seconds == 2.5
    assert cfg.delivery.flush_adaptive_high_seconds == 60.0
    assert cfg.delivery.flush_adaptive_step_rows == 8000
    assert cfg.delivery.flush_adaptive_min_rows == 2000
    assert cfg.delivery.flush_adaptive_reprobe_after == 25


def test_retired_byte_knobs_warn_ignored(tmp_path: Path, caplog):
    # The byte-controller keys were retired 2026-08-28 (flush-sizing
    # endgame): a stale chart carrying them must load fine with the new
    # defaults and a WARN per key — never a refusal (the replacement rows
    # controller is on by default, so there is no silent-safety-loss).
    import logging

    raw = MINIMAL_YAML + (
        "\ndelivery:\n"
        "  flush_max_rows: 500000\n"
        "  flush_max_bytes: 1073741824\n"
        "  flush_adaptive_step_bytes: 8388608\n"
        "  flush_adaptive_min_bytes: 4194304\n"
    )
    p = tmp_path / "retired.yaml"
    p.write_text(raw)
    with caplog.at_level(logging.WARNING, logger="viaduck.config"):
        cfg = load(p)
    assert cfg.delivery.flush_batch_max_rows == 60_000  # untouched default
    warned = [r.message for r in caplog.records if "was removed with the byte-denominated" in r.message]
    assert len(warned) == 4
    assert any("flush_max_bytes" in m for m in warned)


def test_flush_batch_max_rows_zero_rejected():
    # 0 was "unlimited" under the byte controller; it is now the adaptive
    # target's init AND ceiling, where unbounded is undefined (and with the
    # legacy flush_max_rows backstop deleted, no rows bound would remain).
    from viaduck.config import DeliveryConfig

    with pytest.raises(ConfigError, match="flush_batch_max_rows"):
        DeliveryConfig(flush_batch_max_rows=0)


# --- slow-consumer isolation knobs (flush deadline / circuit / cycle budget) ---


def test_slow_consumer_knob_defaults():
    from viaduck.config import DeliveryConfig, PollConfig

    d = DeliveryConfig()
    assert d.flush_deadline_seconds == 0.0  # 0 = derive 2x flush_interval at DeliveryManager init
    assert d.flush_circuit_failures == 3
    assert d.flush_circuit_max_seconds == 900.0
    p = PollConfig()
    assert p.read_unit_max_rows == 50_000
    assert p.read_unit_max_span == 10_000
    assert p.read_workers == 8


def test_slow_consumer_knob_validation():
    from viaduck.config import DeliveryConfig

    with pytest.raises(ConfigError, match="flush_deadline_seconds"):
        DeliveryConfig(flush_deadline_seconds=-1.0)
    with pytest.raises(ConfigError, match="flush_circuit_failures"):
        DeliveryConfig(flush_circuit_failures=0)
    with pytest.raises(ConfigError, match="flush_circuit_max_seconds"):
        DeliveryConfig(flush_circuit_max_seconds=0.5)


def test_slow_consumer_knob_loader_passthrough(tmp_path: Path):
    raw = MINIMAL_YAML + (
        "\npoll:\n"
        "  read_unit_max_rows: 40000\n"
        "delivery:\n"
        "  flush_deadline_seconds: 240.0\n"
        "  flush_circuit_failures: 5\n"
        "  flush_circuit_max_seconds: 300\n"
    )
    p = tmp_path / "slowknobs.yaml"
    p.write_text(raw)
    cfg = load(p)
    assert cfg.poll.read_unit_max_rows == 40_000
    assert cfg.delivery.flush_deadline_seconds == 240.0
    assert cfg.delivery.flush_circuit_failures == 5
    assert cfg.delivery.flush_circuit_max_seconds == 300.0


def test_deleted_poll_knobs_refused_loudly(tmp_path: Path):
    """The retired scheduler's knobs must refuse loudly (a stale chart
    carrying them must not silently get defaults — deploy coupling B1)."""
    for dead in ("cdc_chunk_snapshots", "cycle_time_budget_seconds"):
        p = tmp_path / f"dead-{dead}.yaml"
        p.write_text(MINIMAL_YAML + f"\npoll:\n  {dead}: 120\n")
        with pytest.raises(ConfigError, match=dead):
            load(p)


def test_destination_buffer_max_bytes_loader(tmp_path: Path):
    raw = MINIMAL_YAML.replace(
        "postgres_uri_env: DEST_QW_PG",
        "postgres_uri_env: DEST_QW_PG\n    buffer_max_bytes: 8589934592",
    )
    assert "buffer_max_bytes" in raw  # guard against a silent no-op replace
    p = tmp_path / "dest_cap.yaml"
    p.write_text(raw)
    cfg = load(p)
    assert cfg.destinations[0].buffer_max_bytes == 8589934592


def test_destination_buffer_max_bytes_default_zero(config_file: Path):
    cfg = load(config_file)
    assert cfg.destinations[0].buffer_max_bytes == 0


# --- memory / self-recycle ---


def test_memory_defaults(config_file: Path):
    cfg = load(config_file)
    assert cfg.memory.self_recycle_enabled is True
    assert cfg.memory.self_recycle_rss_fraction == 0.75
    assert cfg.memory.self_recycle_rss_gib == 0.0
    assert cfg.memory.self_recycle_min_uptime_seconds == 3600.0


def test_memory_explicit_values(tmp_path: Path):
    p = tmp_path / "viaduck.yaml"
    p.write_text(
        MINIMAL_YAML
        + """
memory:
  self_recycle_enabled: false
  self_recycle_rss_fraction: 0.5
  self_recycle_rss_gib: 70
  self_recycle_min_uptime_seconds: 0
"""
    )
    cfg = load(p)
    assert cfg.memory.self_recycle_enabled is False
    assert cfg.memory.self_recycle_rss_fraction == 0.5
    assert cfg.memory.self_recycle_rss_gib == 70.0
    assert cfg.memory.self_recycle_min_uptime_seconds == 0.0


@pytest.mark.parametrize(
    "snippet",
    [
        "  self_recycle_rss_fraction: 0",
        "  self_recycle_rss_fraction: 1",
        "  self_recycle_rss_fraction: 1.5",
        "  self_recycle_rss_gib: -1",
        "  self_recycle_min_uptime_seconds: -5",
        "  dest_conn_max_age_seconds: -1",
        "  dest_conn_max_age_seconds: 30",
    ],
)
def test_memory_validation_rejects(tmp_path: Path, snippet: str):
    p = tmp_path / "viaduck.yaml"
    p.write_text(MINIMAL_YAML + "\nmemory:\n" + snippet + "\n")
    with pytest.raises(ConfigError):
        load(p)


def test_memory_dest_conn_max_age_default_and_override(tmp_path: Path):
    p = tmp_path / "viaduck.yaml"
    p.write_text(MINIMAL_YAML)
    assert load(p).memory.dest_conn_max_age_seconds == 600.0

    p.write_text(MINIMAL_YAML + "\nmemory:\n  dest_conn_max_age_seconds: 120\n")
    assert load(p).memory.dest_conn_max_age_seconds == 120.0

    # off is allowed (0 disables the sweep)
    p.write_text(MINIMAL_YAML + "\nmemory:\n  dest_conn_max_age_seconds: 0\n")
    assert load(p).memory.dest_conn_max_age_seconds == 0.0


def test_to_libpq_conninfo_translation():
    """The chart's SOURCE_POSTGRES_URI is DuckDB-ATTACH format; psycopg
    rejects it verbatim (review F2 — the feed must translate like the
    StateManager does)."""
    from viaduck.config import _to_libpq_conninfo

    assert _to_libpq_conninfo("postgres:host=h port=5432 dbname=d user=u") == "host=h port=5432 dbname=d user=u"
    assert _to_libpq_conninfo("postgresql://u@h/d") == "postgresql://u@h/d"
    assert _to_libpq_conninfo("postgres://u@h/d") == "postgres://u@h/d"
    assert _to_libpq_conninfo("host=h dbname=d") == "host=h dbname=d"
