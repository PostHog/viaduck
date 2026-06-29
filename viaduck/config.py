"""YAML config parsing with env var resolution for credentials.

Postgres URI format
-------------------
The env var named by `postgres_uri_env` must contain a DuckDB postgres
extension keyword/value connection string, NOT a libpq URI:

    postgres:host=H port=P user=U password=PWD dbname=DB

The libpq URI form (`postgresql://U:PWD@H:P/DB`) looks valid but DuckLake's
ATTACH does not recognize it and falls through to its file backend, failing
at startup with `Cannot open file: No such file or directory`. viaduck
passes the env value through unchanged to pyducklake; format selection lives
entirely with whatever produces the env var (chart, docker-compose, etc.).
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from viaduck.partition_transforms import TRANSFORM_NAMES as _PARTITION_TRANSFORMS


class ConfigError(Exception):
    pass


def _validate_string_dict(props: object, ctx: str) -> dict[str, str]:
    """Validate that a YAML node is a mapping of str -> str.

    Catches the common YAML foot-gun where `key: true`, `key: 5`, etc. produce
    a non-string value that later code (e.g. `.endswith("_env")`) blows up on.
    """
    if not isinstance(props, dict):
        raise ConfigError(f"{ctx} must be a mapping (got {type(props).__name__})")
    for k, v in props.items():
        if not isinstance(k, str):
            raise ConfigError(f"{ctx}: keys must be strings (got {type(k).__name__})")
        if not isinstance(v, str):
            raise ConfigError(f"{ctx}.{k} must be a string (got {type(v).__name__}); quote scalars in YAML")
    return props  # type: ignore[return-value]


def _validate_int(value: object, ctx: str) -> int:
    """Validate that a YAML node is an integer (and not a bool, which is an int subclass)."""
    # Bool is technically an int subclass in Python; reject it explicitly so
    # `partition.total: true` doesn't silently become 1.
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"{ctx} must be an integer (got {type(value).__name__})")
    return value


def _resolve_env_properties(props: dict[str, str]) -> dict[str, str]:
    """Resolve properties: keys ending in _env have their values read from env vars."""
    resolved = {}
    for key, value in props.items():
        if key.endswith("_env"):
            real_key = key[:-4]  # strip _env suffix
            env_val = os.environ.get(value)
            if env_val is None:
                raise ConfigError(f"Environment variable {value!r} (for property {real_key!r}) is not set")
            resolved[real_key] = env_val
        else:
            resolved[key] = value
    return resolved


def _resolve_env_value(env_var_name: str) -> str:
    """Resolve a single env var reference."""
    val = os.environ.get(env_var_name)
    if val is None:
        raise ConfigError(f"Environment variable {env_var_name!r} is not set")
    return val


def _require_non_empty(value: str, field_name: str) -> str:
    """Validate that a required string field is non-empty."""
    if not value or not value.strip():
        raise ConfigError(f"{field_name!r} must be a non-empty string")
    return value


# Allowed transform names for `partition_by` entries live in
# viaduck/partition_transforms.py — single source of truth shared with
# destination.py's apply path (see imports at the top of this module).
_PARTITION_ENTRY_RE = re.compile(r"^([a-z_][a-z0-9_]*)\(([a-z_][a-z0-9_]*)\)$", re.IGNORECASE)
_PARTITION_IDENT_RE = re.compile(r"^[a-z_][a-z0-9_]*$", re.IGNORECASE)


def _validate_partition_by(value: object, ctx: str) -> tuple[tuple[str, str], ...]:
    """Validate and parse a `partition_by` config entry into a tuple of
    `(transform_name, column_name)` pairs.

    Accepts:
      - A YAML list of strings: `["team_id", "year(_inserted_at)", ...]`
      - Each entry is either a bare column name (identity transform) or
        `func(col)` where func ∈ {year, month, day, hour}

    Returns:
      tuple of (transform_name, column_name) — transform_name is "" for
      identity. The actual pyducklake.Transform mapping happens at apply
      time in destination.py so config.py stays free of pyducklake imports.

    Identifier normalization:
      Column names are lowercase-normalized on parse. This matches
      DuckDB's case-folding behavior for unquoted identifiers: a column
      created as `_inserted_at` is stored case-folded; if config says
      `Year(_Inserted_At)` we normalize to `_inserted_at` so the ALTER
      doesn't fail because pyducklake quotes the name verbatim (and a
      quoted `"_Inserted_At"` is a different column than an unquoted
      `_inserted_at` to DuckDB). Tables created with explicitly quoted
      mixed-case columns need raw-SQL ALTER (not this config path).

    Raises ConfigError on:
      - non-list YAML
      - non-string entries
      - malformed entries (unbalanced parens, illegal identifier chars)
      - unknown transform functions
    """
    if value is None or value == []:
        return ()
    if not isinstance(value, list):
        raise ConfigError(f"{ctx} must be a list of strings (got {type(value).__name__})")
    out: list[tuple[str, str]] = []
    for i, entry in enumerate(value):
        if not isinstance(entry, str):
            raise ConfigError(f"{ctx}[{i}] must be a string (got {type(entry).__name__})")
        entry_stripped = entry.strip()
        if not entry_stripped:
            raise ConfigError(f"{ctx}[{i}] is empty")
        if "(" in entry_stripped or ")" in entry_stripped:
            m = _PARTITION_ENTRY_RE.match(entry_stripped)
            if not m:
                raise ConfigError(f"{ctx}[{i}] = {entry!r} must be 'col' or 'func(col)' where col is an identifier")
            func, col = m.group(1).lower(), m.group(2).lower()
            if func not in _PARTITION_TRANSFORMS:
                raise ConfigError(
                    f"{ctx}[{i}] uses unknown transform {func!r}; "
                    f"supported: {_PARTITION_TRANSFORMS} (or omit for identity)"
                )
            out.append((func, col))
        else:
            if not _PARTITION_IDENT_RE.match(entry_stripped):
                raise ConfigError(f"{ctx}[{i}] = {entry!r} is not a valid column identifier")
            out.append(("", entry_stripped.lower()))
    return tuple(out)


@dataclass(frozen=True)
class SourceConfig:
    name: str
    postgres_uri_env: str
    data_path: str
    table: str
    properties: dict[str, str] = field(default_factory=dict)

    @property
    def postgres_uri(self) -> str:
        return _resolve_env_value(self.postgres_uri_env)

    def resolved_properties(self) -> dict[str, str]:
        return _resolve_env_properties(self.properties)


_VALID_ROUTING_MODES = ("full_cdc", "append_only")


@dataclass(frozen=True)
class RoutingConfig:
    field: str
    # Source-read + apply mode for the pipeline:
    #   full_cdc      — read source via ducklake_table_changes (inserts +
    #                   deletes + update preimages/postimages), run Phase 1
    #                   preimage resolution and Phase 2 conflict resolution,
    #                   apply via tbl.upsert(rows, join_cols=key_columns).
    #                   Requires non-empty key_columns.
    #   append_only   — read source via ducklake_table_insertions (inserts
    #                   only), skip Phase 1 and Phase 2 entirely, apply
    #                   via tbl.append(rows). Requires empty key_columns
    #                   (none of the apply-path machinery uses them, so a
    #                   non-empty value would be silent misconfiguration).
    # Required: no default — the previous "infer full_cdc from len(key_columns)
    # > 0" derivation was a silent misconfig hazard (a typo'd or accidentally-
    # empty list flipped the entire pipeline shape with no operator-visible
    # signal). Explicit mode + a startup error on mismatch keeps the operator
    # honest.
    mode: str = ""
    key_columns: list[str] = field(default_factory=list)
    seed_mode: str = "scan"  # "scan", "earliest", or "latest"
    # REPLACE-semantics seeding: a destination at cursor 0 with existing
    # rows (only legitimate cause under single-master: a crashed prior
    # seed) is truncated before the seed streams — matching the spec's
    # SeedDestination and healing the CrashAfterSeed window. Set False to
    # refuse loudly instead (protects a misconfigured destination pointed
    # at a populated table).
    seed_truncate: bool = True

    def __post_init__(self):
        if not isinstance(self.mode, str):
            # YAML 1.1 coerces bare `yes`/`no`/`on`/`off` to bools, and `1`/`0`
            # to ints, so a typo'd `mode: yes` lands here as Python True and
            # the enum check below would print `got True` — operator-confusing.
            raise ConfigError(
                f"routing.mode must be a string, got {type(self.mode).__name__} ({self.mode!r}). "
                'Quote the value if YAML coerced it (e.g. `mode: "append_only"`).'
            )
        if not self.mode:
            raise ConfigError(
                f"routing.mode is required, no default. Set it to one of {list(_VALID_ROUTING_MODES)}: "
                f"'full_cdc' for sources that emit deletes/updates (requires key_columns), "
                f"'append_only' for insert-only sources (key_columns must be empty)."
            )
        if self.mode not in _VALID_ROUTING_MODES:
            raise ConfigError(
                f"routing.mode must be one of {list(_VALID_ROUTING_MODES)}, got {self.mode!r}. "
                f"Use 'full_cdc' for sources that emit deletes/updates (requires key_columns), "
                f"'append_only' for insert-only sources (key_columns must be empty)."
            )
        if self.mode == "full_cdc" and not self.key_columns:
            raise ConfigError(
                "routing.mode='full_cdc' requires a non-empty routing.key_columns list "
                "(the upsert join keys for the apply path)."
            )
        if self.mode == "append_only" and self.key_columns:
            raise ConfigError(
                "routing.mode='append_only' forbids routing.key_columns; the append path "
                f"does not use them and a non-empty value indicates misconfiguration. "
                f"Got key_columns={self.key_columns!r}; remove them or switch to mode='full_cdc'."
            )
        if self.seed_mode not in ("scan", "earliest", "latest"):
            raise ConfigError(f"routing.seed_mode must be 'scan', 'earliest', or 'latest', got {self.seed_mode!r}")


@dataclass(frozen=True)
class DestinationConfig:
    id: str
    routing_value: str
    name: str
    postgres_uri_env: str
    data_path: str
    table: str
    properties: dict[str, str] = field(default_factory=dict)
    # Optional partition spec applied at table-creation time. Each entry is
    # `(transform_name, column_name)` where transform_name is "" for identity
    # (bare column) or one of {year, month, day, hour}. Empty tuple means
    # the destination table is created (or left) unpartitioned.
    # See destination.py for how this is applied via pyducklake's UpdateSpec.
    partition_by: tuple[tuple[str, str], ...] = ()
    # Safety gate: refuse to ALTER a destination table that already has
    # data files committed. False (default) protects against accidental
    # post-fact partitioning of a populated table — DuckLake's behavior
    # on `ALTER TABLE SET PARTITIONED BY` against a non-empty table is
    # not fully verified at scale and reproducing the mixed-layout
    # compactor breakage we hit on megaduck (2026-06-24..28) is the
    # specific risk we want to gate behind operator opt-in.
    # Set to True only after verifying ALTER behavior on a throwaway
    # copy of the destination AND coordinating a pause of the
    # destination compactor (it groups files by partition_id and trips
    # the line-546 hive-prefix assertion on mixed layouts).
    partition_by_allow_alter_populated: bool = False

    @property
    def postgres_uri(self) -> str:
        return _resolve_env_value(self.postgres_uri_env)

    def resolved_properties(self) -> dict[str, str]:
        return _resolve_env_properties(self.properties)


@dataclass(frozen=True)
class PollConfig:
    interval_seconds: float = 5.0
    cdc_chunk_snapshots: int = 100

    def __post_init__(self):
        if self.cdc_chunk_snapshots < 1:
            raise ConfigError(f"poll.cdc_chunk_snapshots must be >= 1, got {self.cdc_chunk_snapshots}")


@dataclass(frozen=True)
class ServerConfig:
    port: int = 8000


@dataclass(frozen=True)
class WebConfig:
    enabled: bool = True


@dataclass(frozen=True)
class PartitionConfig:
    mode: str = "all"  # "all", "explicit", or "hash"
    include: list[str] = field(default_factory=list)
    total: int = 1
    ordinal: int = 0

    def __post_init__(self):
        if self.mode not in ("all", "explicit", "hash"):
            raise ConfigError(f"partition.mode must be 'all', 'explicit', or 'hash', got {self.mode!r}")
        if self.mode == "explicit" and not self.include:
            raise ConfigError("partition.mode 'explicit' requires a non-empty 'include' list")
        if self.mode == "hash":
            if self.total < 1:
                raise ConfigError(f"partition.total must be >= 1, got {self.total}")
            if not (0 <= self.ordinal < self.total):
                raise ConfigError(f"partition.ordinal must be in [0, {self.total}), got {self.ordinal}")


@dataclass(frozen=True)
class InstanceConfig:
    id: str = "viaduck-0"
    partition: PartitionConfig = field(default_factory=PartitionConfig)


def _to_libpq_conninfo(uri: str) -> str:
    """Translate the source catalog's URI format into something psycopg accepts.

    The source/destination `postgres_uri_env` values use DuckDB's ATTACH
    format: ``postgres:host=H port=P dbname=DB ...`` — a ``postgres:``
    prefix on a libpq keyword/value string. psycopg rejects that verbatim
    (no ``//``, so libpq parses ``postgres:host`` as an invalid keyword).
    Stripping the prefix yields a valid libpq conninfo. Real libpq URIs
    (``postgresql://`` / ``postgres://``) and bare keyword/value strings
    pass through untouched.
    """
    if uri.startswith(("postgresql://", "postgres://")):
        return uri
    if uri.startswith("postgres:"):
        return uri[len("postgres:") :]
    return uri


@dataclass(frozen=True)
class StateConfig:
    table: str = "viaduck_state"
    # Dedicated schema so viaduck's bookkeeping never pollutes the ducklake
    # catalog's namespace, and a future scoped-down PG user has a clean
    # GRANT boundary (USAGE on schema + table grants).
    schema: str = "viaduck"
    # Postgres for the cursor store. Defaults to the source catalog's URI
    # (the same database already hosting the ducklake metadata). Set
    # explicitly when the source catalog isn't Postgres-backed (e.g. a
    # local DuckDB file in dev).
    postgres_uri_env: str | None = None

    def resolve_postgres_uri(self, source: SourceConfig) -> str:
        env = self.postgres_uri_env or source.postgres_uri_env
        return _to_libpq_conninfo(_resolve_env_value(env))


@dataclass(frozen=True)
class DeliveryConfig:
    """Buffered-delivery knobs. CDC reads happen at poll cadence; destination
    writes happen at flush cadence — per-destination buffers accumulate
    between flushes (see tla/Viaduck.tla for the verified semantics).

    workers=1 + flush_interval_seconds=0 reproduces the pre-buffering
    behavior (flush every cycle, serial)."""

    workers: int = 8
    flush_interval_seconds: float = 120.0
    flush_max_rows: int = 500_000
    flush_max_bytes: int = 268_435_456  # 256 MiB per destination
    buffer_total_max_bytes: int = 1_073_741_824  # 1 GiB across all buffers
    pool_max_open: int = 100  # destination connection pool size

    def __post_init__(self):
        if self.workers < 1:
            raise ConfigError(f"delivery.workers must be >= 1, got {self.workers}")
        if self.flush_interval_seconds < 0:
            raise ConfigError(f"delivery.flush_interval_seconds must be >= 0, got {self.flush_interval_seconds}")
        for name in ("flush_max_rows", "flush_max_bytes", "buffer_total_max_bytes", "pool_max_open"):
            if getattr(self, name) < 1:
                raise ConfigError(f"delivery.{name} must be >= 1, got {getattr(self, name)}")


@dataclass(frozen=True)
class ViaduckConfig:
    source: SourceConfig
    routing: RoutingConfig
    destinations: list[DestinationConfig]
    poll: PollConfig = field(default_factory=PollConfig)
    server: ServerConfig = field(default_factory=ServerConfig)
    web: WebConfig = field(default_factory=WebConfig)
    instance: InstanceConfig = field(default_factory=InstanceConfig)
    state: StateConfig = field(default_factory=StateConfig)
    delivery: DeliveryConfig = field(default_factory=DeliveryConfig)

    def __post_init__(self):
        if not self.destinations:
            raise ConfigError("At least one destination is required")
        ids = [d.id for d in self.destinations]
        dupes = [x for x in ids if ids.count(x) > 1]
        if dupes:
            raise ConfigError(f"Duplicate destination IDs: {sorted(set(dupes))}")
        rv = [d.routing_value for d in self.destinations]
        rv_dupes = [x for x in rv if rv.count(x) > 1]
        if rv_dupes:
            raise ConfigError(f"Duplicate routing values: {sorted(set(rv_dupes))}")

    @property
    def pipeline_name(self) -> str:
        return f"{self.source.table}-{self.instance.id}"

    def destination_by_id(self, dest_id: str) -> DestinationConfig:
        for d in self.destinations:
            if d.id == dest_id:
                return d
        raise ConfigError(f"Unknown destination ID: {dest_id!r}")

    def log_summary(self, log: logging.Logger) -> None:
        """Emit one INFO log line per leaf config field. Each value is
        independently greppable from the deploy log — operators can answer
        "what flush_interval_seconds did this pod start with?" or "is the
        fast path on for team-2?" without re-reading the values yaml or
        execing into the pod.

        Resolved secrets (postgres connection strings with credentials, S3
        access keys) are never logged. Only raw dataclass fields are dumped,
        so a `*_env` field holds the env var NAME — safe — while the
        `postgres_uri` @property (which resolves the env var) is not touched.
        `properties` dicts contain the same `*_env` references (YAML literal
        env var names), so they're safe too.
        """
        log.info("config: source.name=%r", self.source.name)
        log.info("config: source.postgres_uri_env=%r", self.source.postgres_uri_env)
        log.info("config: source.data_path=%r", self.source.data_path)
        log.info("config: source.table=%r", self.source.table)
        log.info("config: source.properties=%r", self.source.properties)

        log.info("config: routing.field=%r", self.routing.field)
        log.info("config: routing.mode=%r", self.routing.mode)
        log.info("config: routing.key_columns=%r", self.routing.key_columns)
        log.info("config: routing.seed_mode=%r", self.routing.seed_mode)
        log.info("config: routing.seed_truncate=%s", self.routing.seed_truncate)

        log.info("config: poll.interval_seconds=%s", self.poll.interval_seconds)
        log.info("config: poll.cdc_chunk_snapshots=%d", self.poll.cdc_chunk_snapshots)

        log.info("config: delivery.workers=%d", self.delivery.workers)
        log.info("config: delivery.flush_interval_seconds=%s", self.delivery.flush_interval_seconds)
        log.info("config: delivery.flush_max_rows=%d", self.delivery.flush_max_rows)
        log.info("config: delivery.flush_max_bytes=%d", self.delivery.flush_max_bytes)
        log.info("config: delivery.buffer_total_max_bytes=%d", self.delivery.buffer_total_max_bytes)
        log.info("config: delivery.pool_max_open=%d", self.delivery.pool_max_open)

        log.info("config: server.port=%d", self.server.port)
        log.info("config: web.enabled=%s", self.web.enabled)

        log.info("config: instance.id=%r", self.instance.id)
        log.info("config: instance.partition.mode=%r", self.instance.partition.mode)
        log.info("config: instance.partition.include=%r", self.instance.partition.include)
        log.info("config: instance.partition.total=%d", self.instance.partition.total)
        log.info("config: instance.partition.ordinal=%d", self.instance.partition.ordinal)

        log.info("config: state.table=%r", self.state.table)
        log.info("config: state.schema=%r", self.state.schema)
        log.info("config: state.postgres_uri_env=%r", self.state.postgres_uri_env)

        log.info("config: destinations.count=%d", len(self.destinations))
        for i, d in enumerate(self.destinations):
            log.info("config: destinations[%d].id=%r", i, d.id)
            log.info("config: destinations[%d].routing_value=%r", i, d.routing_value)
            log.info("config: destinations[%d].name=%r", i, d.name)
            log.info("config: destinations[%d].postgres_uri_env=%r", i, d.postgres_uri_env)
            log.info("config: destinations[%d].data_path=%r", i, d.data_path)
            log.info("config: destinations[%d].table=%r", i, d.table)
            log.info("config: destinations[%d].properties=%r", i, d.properties)
            log.info("config: destinations[%d].partition_by=%r", i, d.partition_by)
            log.info(
                "config: destinations[%d].partition_by_allow_alter_populated=%r",
                i,
                d.partition_by_allow_alter_populated,
            )

    def assigned_destination_ids(self) -> list[str]:
        """Return destination IDs assigned to this instance based on partition config."""
        all_ids = [d.id for d in self.destinations]
        mode = self.instance.partition.mode
        if mode == "all":
            return all_ids
        elif mode == "explicit":
            return [did for did in self.instance.partition.include if did in all_ids]
        elif mode == "hash":
            total = self.instance.partition.total
            ordinal = self.instance.partition.ordinal
            return [did for did in all_ids if _stable_hash(did) % total == ordinal]
        return all_ids


def _stable_hash(value: str) -> int:
    """Deterministic hash for partition assignment (not Python's built-in hash which is randomized)."""
    return int(hashlib.sha256(value.encode()).hexdigest(), 16)


def _merge_defaults(dest_props: dict[str, str], default_props: dict[str, str]) -> dict[str, str]:
    """Merge default properties into destination properties (dest takes precedence)."""
    merged = dict(default_props)
    merged.update(dest_props)
    return merged


def load(path: str | Path) -> ViaduckConfig:
    """Load and validate config from a YAML file."""
    path = Path(path)
    if not path.exists():
        raise ConfigError(f"Config file not found: {path}")

    with open(path) as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise ConfigError("Config file must be a YAML mapping")

    # Source
    src = raw.get("source")
    if not src:
        raise ConfigError("'source' section is required")
    source = SourceConfig(
        name=_require_non_empty(src.get("name", ""), "source.name"),
        postgres_uri_env=_require_non_empty(src.get("postgres_uri_env", ""), "source.postgres_uri_env"),
        data_path=_require_non_empty(src.get("data_path", ""), "source.data_path"),
        table=_require_non_empty(src.get("table", ""), "source.table"),
        properties=_validate_string_dict(src.get("properties", {}), "source.properties"),
    )

    # Routing
    rt = raw.get("routing")
    if not rt:
        raise ConfigError("'routing' section is required")
    raw_key_cols = rt.get("key_columns", [])
    if not isinstance(raw_key_cols, list):
        raise ConfigError(f"routing.key_columns must be a list (got {type(raw_key_cols).__name__})")
    if not all(isinstance(k, str) for k in raw_key_cols):
        raise ConfigError("routing.key_columns entries must all be strings")
    routing = RoutingConfig(
        field=_require_non_empty(rt.get("field", ""), "routing.field"),
        # `or ""` so YAML `mode:` (key present, no value → Python None) routes
        # to the "is required, no default" branch rather than the isinstance
        # type-error branch (which would tell the operator to quote `None`).
        mode=rt.get("mode") or "",
        key_columns=raw_key_cols,
        seed_mode=rt.get("seed_mode", "scan"),
        seed_truncate=bool(rt.get("seed_truncate", True)),
    )

    # Defaults
    default_props = _validate_string_dict(raw.get("defaults", {}).get("properties", {}), "defaults.properties")

    # Destinations
    dests_raw = raw.get("destinations", [])
    destinations = []
    for i, d in enumerate(dests_raw):
        dest_props = _merge_defaults(
            _validate_string_dict(d.get("properties", {}), f"destinations[{i}].properties"),
            default_props,
        )
        allow_alter_raw = d.get("partition_by_allow_alter_populated", False)
        if not isinstance(allow_alter_raw, bool):
            raise ConfigError(
                f"destinations[{i}].partition_by_allow_alter_populated must be a boolean "
                f"(got {type(allow_alter_raw).__name__})"
            )
        destinations.append(
            DestinationConfig(
                id=_require_non_empty(str(d.get("id", "")), f"destinations[{i}].id"),
                routing_value=_require_non_empty(str(d.get("routing_value", "")), f"destinations[{i}].routing_value"),
                name=_require_non_empty(d.get("name", ""), f"destinations[{i}].name"),
                postgres_uri_env=_require_non_empty(
                    d.get("postgres_uri_env", ""), f"destinations[{i}].postgres_uri_env"
                ),
                data_path=_require_non_empty(d.get("data_path", ""), f"destinations[{i}].data_path"),
                table=d.get("table", source.table),
                properties=dest_props,
                partition_by=_validate_partition_by(d.get("partition_by"), f"destinations[{i}].partition_by"),
                partition_by_allow_alter_populated=allow_alter_raw,
            )
        )

    # Optional sections
    poll_raw = raw.get("poll", {})
    poll = PollConfig(
        interval_seconds=poll_raw.get("interval_seconds", 5.0),
        cdc_chunk_snapshots=_validate_int(poll_raw.get("cdc_chunk_snapshots", 100), "poll.cdc_chunk_snapshots"),
    )

    server_raw = raw.get("server", {})
    server = ServerConfig(port=server_raw.get("port", 8000))

    web_raw = raw.get("web", {})
    web = WebConfig(enabled=web_raw.get("enabled", True))

    state_raw = raw.get("state", {})
    state = StateConfig(
        table=state_raw.get("table", "viaduck_state"),
        schema=state_raw.get("schema", "viaduck"),
        postgres_uri_env=state_raw.get("postgres_uri_env") or None,
    )

    delivery_raw = raw.get("delivery", {})
    delivery = DeliveryConfig(
        workers=_validate_int(delivery_raw.get("workers", 8), "delivery.workers"),
        flush_interval_seconds=float(delivery_raw.get("flush_interval_seconds", 120.0)),
        flush_max_rows=_validate_int(delivery_raw.get("flush_max_rows", 500_000), "delivery.flush_max_rows"),
        flush_max_bytes=_validate_int(delivery_raw.get("flush_max_bytes", 268_435_456), "delivery.flush_max_bytes"),
        buffer_total_max_bytes=_validate_int(
            delivery_raw.get("buffer_total_max_bytes", 1_073_741_824), "delivery.buffer_total_max_bytes"
        ),
        pool_max_open=_validate_int(delivery_raw.get("pool_max_open", 100), "delivery.pool_max_open"),
    )

    inst_raw = raw.get("instance", {})
    part_raw = inst_raw.get("partition", {})
    partition = PartitionConfig(
        mode=part_raw.get("mode", "all"),
        include=part_raw.get("include", []),
        total=_validate_int(part_raw.get("total", 1), "partition.total"),
        ordinal=_validate_int(part_raw.get("ordinal", 0), "partition.ordinal"),
    )
    instance = InstanceConfig(
        id=inst_raw.get("id", "viaduck-0"),
        partition=partition,
    )

    return ViaduckConfig(
        source=source,
        routing=routing,
        destinations=destinations,
        poll=poll,
        server=server,
        web=web,
        instance=instance,
        state=state,
        delivery=delivery,
    )
