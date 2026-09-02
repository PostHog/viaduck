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

log = logging.getLogger(__name__)


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


def _validate_non_negative_int(value: object, ctx: str) -> int:
    """Like _validate_int but also rejects negatives: a typo'd negative on
    a per-destination override would otherwise silently fall through to
    the global default — the exact misconfiguration the override surface
    exists to prevent."""
    v = _validate_int(value, ctx)
    if v < 0:
        raise ConfigError(f"{ctx} must be >= 0 (0 = use the global default), got {v}")
    return v


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
    # CDC reads are always the direct-SQL feed (viaduck/feed.py) for
    # append_only; full_cdc sources keep the extension changefeed (the feed
    # has no delete stream). The retired source.cdc_reader knob is inert in
    # configs that still render it (the loader tolerates unknown keys).

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
class DeferredUriSource:
    """Connection parts + k8s Secret reference for a discovered
    destination whose password is resolved at connection-create time
    (C3 §5 secret-ref deferral) instead of baked into a URI at startup.
    Field names deliberately mirror discovery.MappedDestination so
    discovery.build_attach_uri accepts either. Carries NO credential
    material — safe to repr/log."""

    pg_endpoint: str
    pg_port: int
    pg_database: str
    pg_username: str
    secret_namespace: str
    secret_name: str
    secret_key: str
    sslmode: str = "disable"


@dataclass(frozen=True)
class DestinationConfig:
    id: str
    routing_value: str
    name: str
    postgres_uri_env: str
    data_path: str
    table: str
    properties: dict[str, str] = field(default_factory=dict)
    # Optional per-destination buffer/queue cap override (bytes). 0 = use
    # the global delivery.buffer_max_bytes_per_destination (or its
    # fair-share auto-derivation; negative values are rejected at load).
    # The cap is a CATCH-UP THROUGHPUT knob:
    # it bounds what this destination's CDC queue (buffer + in-flight)
    # can absorb per read turn — deep-laggard catch-up rate is
    # proportional to it. Aggregate memory stays governed by
    # delivery.buffer_total_max_bytes via watermark force-flushes.
    # Rationale for per-destination shape: destination volumes are
    # heavily skewed; one global number is either wasteful for the tail
    # or throttling for the head (2026-08-01 sizing).
    buffer_max_bytes: int = 0
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
    # Schema projection: when writing into a destination whose Arrow schema
    # doesn't match the source (canonical `posthog.events` — DLT-backfilled
    # timestamptz shape — vs the raw-Kafka `events_nrt` shape millpond
    # emits), build a projection at pool-open time and apply it before each
    # write. See viaduck/schema_projection.py. Off by default; identity is
    # the historical behavior.
    schema_projection_enabled: bool = False
    # Source columns to silently drop as part of the projection. Empty by
    # default — the projection raises `SchemaProjectionError` at pool-open
    # time on any source column not in the target and not in this list.
    # Typical entries: `captured_at` when writing to canonical events.
    drop_source_columns: tuple[str, ...] = ()
    # Directly-provided postgres URI (CP-discovered destinations build it
    # from the discovery payload + a k8s Secret read; static destinations
    # keep the env indirection). NEVER log this value — unlike
    # postgres_uri_env it IS the credential, not a pointer to one.
    postgres_uri_direct: str | None = field(default=None, repr=False)
    # Deferred credential resolution (C3 §5): discovered destinations
    # carry the secret REF + connection parts; DestinationPool._create
    # resolves the password (TTL-cached, stale-fallback) and builds the
    # attach URI per connect. Password rotation heals via ordinary
    # evict/recreate instead of a pod restart. Mutually exclusive with
    # postgres_uri_direct in practice (discovery sets exactly one).
    uri_source: DeferredUriSource | None = None

    @property
    def postgres_uri(self) -> str:
        if self.uri_source is not None:
            raise ConfigError(
                f"destination {self.id!r} uses deferred credential resolution; "
                "the attach URI must be built at connection-create time (DestinationPool._create)"
            )
        if self.postgres_uri_direct is not None:
            return self.postgres_uri_direct
        return _resolve_env_value(self.postgres_uri_env)

    def resolved_properties(self) -> dict[str, str]:
        return _resolve_env_properties(self.properties)


@dataclass(frozen=True)
class PollConfig:
    interval_seconds: float = 5.0
    # Read loop (log-consumer-proposal.md §6.3): each FEED read unit
    # (append_only) is bounded by rows AND bytes AND snapshot span; the
    # full_cdc extension path has no per-row attribution to budget on and
    # is span-only, hard-capped at 480 snapshots per unit (main.py
    # read-unit planning). UNIT CONTRACT: read_unit_max_bytes is priced in
    # parquet file_size_bytes from the catalog — compressed, on-wire — NOT
    # decoded Arrow memory, which can exceed it 20-100x for
    # dictionary-heavy shapes (the 2026-08 unit-lie class; the flush side
    # retired byte-denominated control over it). Inline rows price at 0
    # bytes. Rows and span are the actual memory backstops; the row budget
    # also keeps a unit's flush under the destination commit cliff
    # (measured: 56.5s @60-90k rows vs the 240s deadline). Units are the
    # read-amortization knob — the retired cdc_chunk_snapshots coupled
    # read size to the flush floor, which is exactly the wedge it caused.
    read_unit_max_rows: int = 50_000
    read_unit_max_bytes: int = 256 * 1024 * 1024
    read_unit_max_span: int = 10_000
    # Parallel read pool size (bare duckdb connections; one in-flight read
    # per destination regardless — tla/ViaduckReads.tla).
    read_workers: int = 8
    # Per-unit read wall-clock ceiling. A wedged read (S3 stall, slow catalog)
    # fails contained and retries next cycle — without it the barrier can
    # hang the poll thread behind a heartbeat-green liveness probe for hours
    # (review O2).
    read_unit_timeout_seconds: float = 300.0

    def __post_init__(self):
        if self.read_unit_max_rows < 1:
            raise ConfigError(f"poll.read_unit_max_rows must be >= 1, got {self.read_unit_max_rows}")
        if self.read_unit_max_span < 1:
            raise ConfigError(f"poll.read_unit_max_span must be >= 1, got {self.read_unit_max_span}")
        if self.read_unit_max_bytes < 1:
            raise ConfigError(f"poll.read_unit_max_bytes must be >= 1, got {self.read_unit_max_bytes}")
        if self.read_workers < 1:
            raise ConfigError(f"poll.read_workers must be >= 1, got {self.read_workers}")
        if self.read_unit_timeout_seconds <= 0:
            raise ConfigError(f"poll.read_unit_timeout_seconds must be > 0, got {self.read_unit_timeout_seconds}")


@dataclass(frozen=True)
class MemoryConfig:
    # Watermark self-recycle: the shipped ducklake extension still accrues
    # untracked native memory (~2.5-4 GiB/h on prod; see hypothesis-1.md
    # residual) with a horizon of roughly a day per pod. A mid-flight OOM
    # rewinds every destination to its durable cursor and tips the
    # cursor-group system into the degraded scattered regime (2026-07-31 and
    # 2026-08-14 incidents); a CLEAN exit after drain() leaves cursors tight
    # at the read position, so the kubelet restart resumes with no rewind.
    # When RSS crosses the watermark, finish the current poll cycle, drain,
    # exit 0.
    self_recycle_enabled: bool = True
    # Watermark as a fraction of the cgroup memory limit. Used when
    # self_recycle_rss_gib is 0 and the limit is readable; if the limit is
    # unreadable/unlimited the recycle is disabled (logged at startup).
    # SIZING: the watermark must clear the deployment's LEGITIMATE peak —
    # roughly delivery.buffer_total_max_bytes + the pool's native footprint
    # + process baseline — or leak-free load recycles the pod every
    # min-uptime. On sized deployments prefer the absolute knob, set from
    # that envelope plus headroom (prod-us: ~74GiB envelope on a 96Gi pod
    # → 0.75 x limit is BELOW it; the chart sets self_recycle_rss_gib
    # explicitly instead).
    self_recycle_rss_fraction: float = 0.75
    # Absolute watermark override in GiB; 0 derives from the fraction.
    self_recycle_rss_gib: float = 0.0
    # Never recycle a young process: a post-restart catch-up legitimately
    # runs hot, and a too-eager watermark would flap-restart into the exact
    # churn this feature exists to avoid.
    self_recycle_min_uptime_seconds: float = 3600.0
    # EXPERIMENTAL source-connection recycle (the Leak-2 discriminating
    # test, persistent_oom.md §The discriminating experiment): 0 disables;
    # >0 closes and reopens the long-lived SOURCE catalog connection at a
    # poll-cycle boundary with zero outstanding read futures, DETACH-first
    # (Leak A lesson), interleaving flush submissions on any reopen retry.
    # Verification is a multi-hour RSS SLOPE comparison, not an
    # instantaneous before/after (allocator slack confounds the latter —
    # see delivery-endgame C3 [R1-2]); viaduck_rss_bytes is exported every
    # cycle so the slope is measurable from metrics alone.
    source_conn_recycle_interval_seconds: float = 0.0
    # Dest-connection age sweep (persistent_oom.md Leak-2): force-evict
    # pooled destination connections older than this many seconds — the
    # close frees accumulated per-connection engine state. Local evidence:
    # first sweep freed ~0.96 GiB instantly; an N-scale measurement
    # (N ∈ {1,5,10}) fit RSS(N) ≈ 0.92 + 0.062·N GiB (R²=0.87) — but that
    # fit is a per-connection LEVEL at steady state, consistent with a
    # fixed ~63 MiB per-connection cost alone; it does NOT by itself prove
    # dest-scope for the prod leak rate. The falsifier (does a 60s sweep
    # zero the slope?) is the next measurement. 600s default bounds the
    # storm layer hypothesis-2 measured (~460MB/90s/contended connection)
    # at ~10 min of accumulation per connection. 0 disables; the watermark
    # self-recycle above stays the backstop for anything the sweep misses.
    dest_conn_max_age_seconds: float = 600.0

    def __post_init__(self):
        if not 0.0 < self.self_recycle_rss_fraction < 1.0:
            raise ConfigError(
                f"memory.self_recycle_rss_fraction must be in (0, 1), got {self.self_recycle_rss_fraction}"
            )
        if self.self_recycle_rss_gib < 0:
            raise ConfigError(f"memory.self_recycle_rss_gib must be >= 0, got {self.self_recycle_rss_gib}")
        if self.self_recycle_min_uptime_seconds < 0:
            raise ConfigError(
                f"memory.self_recycle_min_uptime_seconds must be >= 0, got {self.self_recycle_min_uptime_seconds}"
            )
        if self.source_conn_recycle_interval_seconds < 0:
            raise ConfigError(
                f"memory.source_conn_recycle_interval_seconds must be >= 0, "
                f"got {self.source_conn_recycle_interval_seconds}"
            )
        if self.dest_conn_max_age_seconds < 0:
            raise ConfigError(f"memory.dest_conn_max_age_seconds must be >= 0, got {self.dest_conn_max_age_seconds}")
        if 0 < self.dest_conn_max_age_seconds < 60:
            raise ConfigError(
                f"memory.dest_conn_max_age_seconds must be 0 (off) or >= 60 "
                f"(below the poll/flush timescale the sweep becomes a connect-storm DoS), "
                f"got {self.dest_conn_max_age_seconds}"
            )


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
    # NOTE (2026-08-28, flush-sizing endgame): byte-denominated flush sizing
    # was retired — no single correct "bytes" exists for an arrow table
    # (in-memory vs decoded-logical vs parquet-on-wire differ by orders of
    # magnitude per shape; three incidents in three days). Flush control is
    # ROW-denominated end to end: the trigger, the slice, and the adaptive
    # target. Buffer CAPS below stay on raw nbytes by contract (inflated —
    # the conservative direction for a memory bound).
    buffer_total_max_bytes: int = 1_073_741_824  # 1 GiB across all buffers — raw nbytes, see above
    # Per-destination buffer + in-flight byte ceiling — the bound on each
    # destination's CDC "queue". When a destination's (buffered + in-flight)
    # bytes reach this, the poll thread stops reading FOR THAT DESTINATION
    # only; peers keep flowing at their own pace. This is what makes
    # backpressure destination-local (Kafka-partition semantics) instead of
    # the previous global watermark, where one stuck destination's bytes
    # paused reads for everyone. 0 (default) auto-derives a fair share:
    # buffer_total_max_bytes / number of assigned destinations.
    buffer_max_bytes_per_destination: int = 0
    # Ceiling on the rows ONE flush takes from the buffer, AND the
    # init/ceiling of the per-destination adaptive rows target below
    # (sliced at entry boundaries, or within an oversize entry; the
    # remainder stays buffered and goes out in the next flush). Without
    # it, a slow flush lets the buffer pile up and the NEXT swap takes
    # everything — the feedback loop that produced 170-440K-row append
    # batches and drove the fork's native layer into buffer-manager
    # corruption + SIGSEGV (2026-07-29 incident). Batches <=~60K rows are
    # the empirically stable regime. Must be >= 1: it is the controller's
    # ceiling, and an unbounded ceiling is undefined.
    flush_batch_max_rows: int = 60_000
    # Adaptive per-destination flush sizing (AIMD on flush DURATION — the
    # unit-proof feedback signal; the actuator is ROWS). The sustainable
    # batch size is a property of each DESTINATION's catalog: on a
    # commit-contended catalog write throughput DECREASES with batch size
    # (a longer write+commit window collides with more peer commits → more
    # DuckLake-internal OCC retries, each re-running multi-second catalog
    # SQL), while an idle catalog absorbs the full-size batch at wire
    # speed. team-2 vs team-50689 (2026-07-30): same row sizes, opposite
    # needs — no single global value serves both. Each destination carries
    # an in-memory ROWS target that starts at flush_batch_max_rows and
    # adapts: in the [low, high] seconds band → hold; faster than low with
    # a >=70%-full batch → additive step_rows (capped at
    # flush_batch_max_rows); slower than high → halve (floored at
    # min_rows). The [low, high] dead zone would be a one-way RATCHET
    # without the re-probe: after flush_adaptive_reprobe_after consecutive
    # in-band, >=70%-full, successful flushes, the target steps up once —
    # healing ratchet-downs from transient contention spikes without
    # waiting for a restart. flush_adaptive=false: fixed target at the
    # ceiling.
    flush_adaptive: bool = True
    flush_adaptive_low_seconds: float = 5.0
    flush_adaptive_high_seconds: float = 30.0
    # Additive growth step in ROWS (default = ceiling/15 — the old
    # 16MiB/256MiB byte-step ratio).
    flush_adaptive_step_rows: int = 4_000
    # Target floor in ROWS; clamped to flush_batch_max_rows at runtime so
    # lowering the ceiling below the floor stays a one-knob change.
    flush_adaptive_min_rows: int = 4_000
    # Consecutive in-band, >=70%-full, successful flushes before the
    # controller re-probes upward one step (the dead-zone ratchet heal).
    flush_adaptive_reprobe_after: int = 50
    pool_max_open: int = 100  # destination connection pool size
    # Overall per-flush wall-clock deadline. The OCC retry loop
    # (apply._write_with_retry) is bounded in ATTEMPTS but unbounded in
    # wall time: 15 attempts x up-to-30s backoff is ~5.5 min of sleeps
    # alone, plus per-attempt write time on a contended catalog — one
    # pathological destination holds a shared flush worker the whole time
    # (the write side is shared fate: one FIFO ThreadPoolExecutor for all
    # destinations). The deadline aborts the retry loop (same FlushFail
    # semantics as any other write failure: buffer drop + range re-read).
    # Scope: the WRITE retry loop only — a single blocking attempt is NOT
    # preempted, and the cursor-persist retry tail runs outside it, so
    # worst-case worker occupancy is deadline + one in-flight attempt +
    # cursor tail. 0 (default) derives 2 x flush_interval_seconds; if the
    # derived value is <= 0 (e.g. flush_interval_seconds=0), the deadline
    # is disabled.
    flush_deadline_seconds: float = 0.0
    # Per-destination flush circuit breaker. After this many CONSECUTIVE
    # flush failures, submissions for that destination pause: without it, a
    # broken destination cycles read->buffer->fail->rewind->re-read
    # forever, burning a flush worker for minutes per attempt AND its
    # per-cycle chunk quota (lagging groups are served first) — one broken
    # destination actively taxes every peer instead of just stalling
    # itself. While open, reads continue under the normal buffer-cap rules
    # (the destination pauses itself at cap, Kafka-partition semantics).
    # Only apply-phase (destination-write) failures count — a cursor-persist
    # failure is shared PG infrastructure, not a sick destination. Only a
    # flush carrying data closes the circuit (a position-only persist never
    # touches the destination). drain() bypasses the gate so shutdown
    # flushes always attempt. Resubmission backs off exponentially:
    # flush_interval_seconds x 2^(failures - threshold), floored at 1s (the
    # raw formula collapses to 0 at flush_interval_seconds=0), capped at
    # flush_circuit_max_seconds; the first submission after the backoff is
    # a probe (in-flight guard makes it one) that closes the circuit on
    # success or re-opens it with the next backoff step on failure.
    flush_circuit_failures: int = 3
    flush_circuit_max_seconds: float = 900.0

    def __post_init__(self):
        if self.workers < 1:
            raise ConfigError(f"delivery.workers must be >= 1, got {self.workers}")
        if self.flush_interval_seconds < 0:
            raise ConfigError(f"delivery.flush_interval_seconds must be >= 0, got {self.flush_interval_seconds}")
        for name in ("buffer_total_max_bytes", "pool_max_open"):
            if getattr(self, name) < 1:
                raise ConfigError(f"delivery.{name} must be >= 1, got {getattr(self, name)}")
        if self.buffer_max_bytes_per_destination < 0:
            raise ConfigError(
                f"delivery.buffer_max_bytes_per_destination must be >= 0 (0 = auto), "
                f"got {self.buffer_max_bytes_per_destination}"
            )
        if self.flush_batch_max_rows < 1:
            # The adaptive controller's init AND ceiling — 0 would leave no
            # rows bound at all (the legacy flush_max_rows backstop is
            # deleted), in either adaptive or fixed mode.
            raise ConfigError(
                f"delivery.flush_batch_max_rows must be >= 1 (it is the flush target's ceiling), "
                f"got {self.flush_batch_max_rows}"
            )
        for name in ("flush_adaptive_step_rows", "flush_adaptive_min_rows", "flush_adaptive_reprobe_after"):
            if getattr(self, name) < 1:
                raise ConfigError(f"delivery.{name} must be >= 1, got {getattr(self, name)}")
        if self.flush_adaptive_low_seconds < 0:
            raise ConfigError(
                f"delivery.flush_adaptive_low_seconds must be >= 0, got {self.flush_adaptive_low_seconds}"
            )
        if self.flush_adaptive_high_seconds <= self.flush_adaptive_low_seconds:
            raise ConfigError(
                f"delivery.flush_adaptive_high_seconds ({self.flush_adaptive_high_seconds}) must be > "
                f"flush_adaptive_low_seconds ({self.flush_adaptive_low_seconds})"
            )
        if self.flush_deadline_seconds < 0:
            raise ConfigError(
                f"delivery.flush_deadline_seconds must be >= 0 (0 = 2x flush_interval_seconds), "
                f"got {self.flush_deadline_seconds}"
            )
        if self.flush_circuit_failures < 1:
            raise ConfigError(f"delivery.flush_circuit_failures must be >= 1, got {self.flush_circuit_failures}")
        if self.flush_circuit_max_seconds < 1:
            raise ConfigError(f"delivery.flush_circuit_max_seconds must be >= 1, got {self.flush_circuit_max_seconds}")


@dataclass(frozen=True)
class DiscoveryConfig:
    """CP-driven destination discovery (viaduck/discovery.py). Disabled by
    default: enabling requires a URL; auth arrives as header name + a
    token env var (the scoped read-only secret — never the CP admin
    internal secret). `defaults` seeds every discovered destination
    (memory_limit, sslmode, extra properties, drop_source_columns)."""

    enabled: bool = False
    url: str | None = None
    auth_header_name: str | None = None
    auth_token_env: str | None = None
    poll_interval_s: float = 60.0
    request_timeout_s: float = 10.0
    # C3 reconciler (viaduck/reconciler.py). Default OFF: the classified
    # view publishes and its metrics tick, but nothing acts on it until
    # this flips (restart-to-flip: config is frozen and loaded once; the
    # flip is a values change -> ConfigMap checksum -> rolling restart).
    apply_enabled: bool = False
    # Stop debounce: a RUNNING discovered destination deactivates only
    # after this many CONSECUTIVE clean fetches (successful, un-poisoned
    # views) observe it absent. A mentioned id (startable or not) resets
    # its counter; failed fetches and poisoned views freeze all counters.
    absent_stop_fetches: int = 3
    # Mass-stop floor: refuse the stop half of a view that would
    # deactivate more than max(1, ceil(fraction * running-discovered))
    # workers (min_destinations is the absolute floor). Activations and
    # restarts from the same view still apply. Availability guard, not a
    # data guard — a false stop self-heals from the cursor within source
    # retention.
    stop_floor_fraction: float = 0.5
    # Rate cap on config-swap restarts (reshard completions): each pool
    # evict/recreate cycle costs ~160MB to the fork-side leak, so bound
    # restarts per unit time rather than per cycle.
    restart_min_interval_s: float = 300.0
    # Refuse a payload mapping fewer destinations than this at startup
    # (fail to static-only, synced=0): a CP bug serving an empty list must
    # not silently vanish every discovered tenant on the next restart.
    # 0 = allow empty (genuine zero-tenant bootstrap).
    min_destinations: int = 1
    # Total wall-time budget for resolving discovered destinations'
    # Secrets at startup (N sequential k8s API reads must never eat the
    # liveness grace and crashloop static tenants).
    materialize_deadline_s: float = 60.0
    # Defense-in-depth against a spoofed/compromised discovery payload:
    # the payload directs which Secret to read and where to send the
    # resulting password (libpq handshake), so both the metadata-store
    # endpoints and the Secret namespaces are allowlisted. Empty list =
    # allow anything (NOT recommended outside tests).
    allowed_endpoint_suffixes: tuple[str, ...] = (".ducklings.svc", ".ducklings.svc.cluster.local")
    allowed_secret_namespaces: tuple[str, ...] = ("ducklings",)
    # TTL for the k8s Secret read cache on the pool's connect path
    # (viaduck/k8s_secrets.py). Discovered destinations carry a secret
    # REF, resolved at connection-create time — with fleets above
    # pool_max_open, LRU churn makes that a routine flush-path event, so
    # the cache (with stale-fallback on API failure) keeps an API-server
    # outage from becoming fleet-wide flush failures. Password rotation
    # heals within one TTL, or immediately via the flush-failure
    # evict/recreate path (which hits the API once the TTL lapses).
    secret_cache_ttl_s: float = 300.0
    defaults: dict = field(default_factory=dict)

    def __post_init__(self):
        if self.enabled and not self.url:
            raise ConfigError("discovery.enabled requires discovery.url")
        if (self.auth_header_name is None) != (self.auth_token_env is None):
            raise ConfigError("discovery.auth_header_name and discovery.auth_token_env must be set together")
        if self.poll_interval_s <= 0 or self.request_timeout_s <= 0 or self.materialize_deadline_s <= 0:
            raise ConfigError("discovery poll_interval_s/request_timeout_s/materialize_deadline_s must be positive")
        if self.secret_cache_ttl_s <= 0:
            raise ConfigError("discovery.secret_cache_ttl_s must be positive")
        if self.absent_stop_fetches < 1:
            raise ConfigError("discovery.absent_stop_fetches must be >= 1")
        if not (0.0 < self.stop_floor_fraction <= 1.0):
            raise ConfigError("discovery.stop_floor_fraction must be in (0, 1]")
        if self.restart_min_interval_s < 0:
            raise ConfigError("discovery.restart_min_interval_s must be >= 0")
        if self.min_destinations < 0:
            raise ConfigError("discovery.min_destinations must be >= 0")
        d = self.defaults
        if not isinstance(d, dict):
            raise ConfigError("discovery.defaults must be a mapping")
        for key in ("memory_limit", "sslmode"):
            if key in d and not isinstance(d[key], str):
                raise ConfigError(f"discovery.defaults.{key} must be a string")
        if "drop_source_columns" in d:
            v = d["drop_source_columns"]
            if not isinstance(v, list) or not all(isinstance(c, str) for c in v):
                # A bare string would iterate per-CHARACTER into the drop
                # list — the same YAML foot-gun the loader guards against
                # for static destinations.
                raise ConfigError("discovery.defaults.drop_source_columns must be a list of strings")
        if "properties" in d:
            _validate_string_dict(d["properties"], "discovery.defaults.properties")

    def auth_header(self) -> tuple[str, str] | None:
        if self.auth_header_name is None or self.auth_token_env is None:
            return None
        return (self.auth_header_name, _resolve_env_value(self.auth_token_env))


@dataclass(frozen=True)
class ViaduckConfig:
    source: SourceConfig
    routing: RoutingConfig
    destinations: list[DestinationConfig]
    poll: PollConfig = field(default_factory=PollConfig)
    server: ServerConfig = field(default_factory=ServerConfig)
    memory: MemoryConfig = field(default_factory=MemoryConfig)
    web: WebConfig = field(default_factory=WebConfig)
    instance: InstanceConfig = field(default_factory=InstanceConfig)
    state: StateConfig = field(default_factory=StateConfig)
    delivery: DeliveryConfig = field(default_factory=DeliveryConfig)
    discovery: DiscoveryConfig = field(default_factory=DiscoveryConfig)

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
        log.info(
            "config: poll.read_unit max_rows=%d max_bytes=%d max_span=%d read_workers=%d",
            self.poll.read_unit_max_rows,
            self.poll.read_unit_max_bytes,
            self.poll.read_unit_max_span,
            self.poll.read_workers,
        )

        log.info("config: delivery.workers=%d", self.delivery.workers)
        log.info("config: delivery.flush_interval_seconds=%s", self.delivery.flush_interval_seconds)
        log.info("config: delivery.flush_batch_max_rows=%d", self.delivery.flush_batch_max_rows)
        log.info("config: delivery.buffer_total_max_bytes=%d", self.delivery.buffer_total_max_bytes)
        log.info(
            "config: delivery.buffer_max_bytes_per_destination=%d (0=auto: total/N)",
            self.delivery.buffer_max_bytes_per_destination,
        )
        log.info(
            "config: delivery.flush_adaptive=%s (band=[%s, %s]s, step_rows=%d, min_rows=%d, reprobe_after=%d)",
            self.delivery.flush_adaptive,
            self.delivery.flush_adaptive_low_seconds,
            self.delivery.flush_adaptive_high_seconds,
            self.delivery.flush_adaptive_step_rows,
            self.delivery.flush_adaptive_min_rows,
            self.delivery.flush_adaptive_reprobe_after,
        )
        log.info("config: delivery.pool_max_open=%d", self.delivery.pool_max_open)
        log.info(
            "config: delivery.flush_deadline_seconds=%s (0=derive: 2x flush_interval)",
            self.delivery.flush_deadline_seconds,
        )
        log.info(
            "config: delivery.flush_circuit_failures=%d, flush_circuit_max_seconds=%s",
            self.delivery.flush_circuit_failures,
            self.delivery.flush_circuit_max_seconds,
        )

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
        return [did for did in (d.id for d in self.destinations) if self.is_assigned(did)]

    def is_assigned(self, dest_id: str) -> bool:
        """Per-id assignment predicate — the same rule assigned_destination_ids
        applies, usable for ids that are not in the startup config (the
        reconciler's dynamically discovered destinations; without this
        every instance of a hash-partitioned fleet would adopt every new
        tenant)."""
        mode = self.instance.partition.mode
        if mode == "explicit":
            return dest_id in self.instance.partition.include
        if mode == "hash":
            return _stable_hash(dest_id) % self.instance.partition.total == self.instance.partition.ordinal
        return True


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
        schema_proj_raw = d.get("schema_projection_enabled", False)
        if not isinstance(schema_proj_raw, bool):
            raise ConfigError(
                f"destinations[{i}].schema_projection_enabled must be a boolean (got {type(schema_proj_raw).__name__})"
            )
        drop_cols_raw = d.get("drop_source_columns", [])
        if not isinstance(drop_cols_raw, list) or not all(isinstance(c, str) for c in drop_cols_raw):
            raise ConfigError(
                f"destinations[{i}].drop_source_columns must be a list of strings (got {type(drop_cols_raw).__name__})"
            )
        # Interlock: `drop_source_columns` is meaningless without projection,
        # and reading as a no-op is a silent-corruption footgun — the operator
        # thinks the drop is happening but the shipped write path is still
        # positional over the source schema. Refuse at load time.
        if drop_cols_raw and not schema_proj_raw:
            raise ConfigError(
                f"destinations[{i}].drop_source_columns={drop_cols_raw!r} is non-empty but "
                f"schema_projection_enabled=false; the drop would be silently ignored. "
                f"Set schema_projection_enabled=true or remove drop_source_columns."
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
                buffer_max_bytes=_validate_non_negative_int(
                    d.get("buffer_max_bytes", 0), f"destinations[{i}].buffer_max_bytes"
                ),
                partition_by=_validate_partition_by(d.get("partition_by"), f"destinations[{i}].partition_by"),
                partition_by_allow_alter_populated=allow_alter_raw,
                schema_projection_enabled=schema_proj_raw,
                drop_source_columns=tuple(drop_cols_raw),
            )
        )

    # Optional sections
    poll_raw = raw.get("poll", {})
    # Deleted knobs refuse loudly (a stale chart carrying them must not
    # silently get defaults — the failure mode is an operator believing the
    # scheduler is bounded when the scheduler no longer exists).
    for dead in ("cdc_chunk_snapshots", "cycle_time_budget_seconds"):
        if dead in poll_raw:
            raise ConfigError(
                f"poll.{dead} was removed in M4 (unit-based reads supersede it): "
                f"delete it from the values file. See log-consumer-proposal.md."
            )
    poll = PollConfig(
        interval_seconds=poll_raw.get("interval_seconds", 5.0),
        read_unit_max_rows=_validate_int(poll_raw.get("read_unit_max_rows", 50_000), "poll.read_unit_max_rows"),
        read_unit_max_bytes=_validate_int(
            poll_raw.get("read_unit_max_bytes", 256 * 1024 * 1024), "poll.read_unit_max_bytes"
        ),
        read_unit_max_span=_validate_int(poll_raw.get("read_unit_max_span", 10_000), "poll.read_unit_max_span"),
        read_workers=_validate_int(poll_raw.get("read_workers", 8), "poll.read_workers"),
        read_unit_timeout_seconds=float(poll_raw.get("read_unit_timeout_seconds", 300.0)),
    )

    server_raw = raw.get("server", {})
    server = ServerConfig(port=server_raw.get("port", 8000))

    memory_raw = raw.get("memory", {})
    memory = MemoryConfig(
        self_recycle_enabled=bool(memory_raw.get("self_recycle_enabled", True)),
        self_recycle_rss_fraction=float(memory_raw.get("self_recycle_rss_fraction", 0.75)),
        self_recycle_rss_gib=float(memory_raw.get("self_recycle_rss_gib", 0.0)),
        self_recycle_min_uptime_seconds=float(memory_raw.get("self_recycle_min_uptime_seconds", 3600.0)),
        source_conn_recycle_interval_seconds=float(
            os.environ.get(
                "SOURCE_CONN_RECYCLE_INTERVAL_SECONDS",
                memory_raw.get("source_conn_recycle_interval_seconds", 0.0),
            )
        ),
        dest_conn_max_age_seconds=float(
            os.environ.get("DEST_CONN_MAX_AGE_SECONDS", memory_raw.get("dest_conn_max_age_seconds", 600.0))
        ),
    )

    web_raw = raw.get("web", {})
    web = WebConfig(enabled=web_raw.get("enabled", True))

    state_raw = raw.get("state", {})
    state = StateConfig(
        table=state_raw.get("table", "viaduck_state"),
        schema=state_raw.get("schema", "viaduck"),
        postgres_uri_env=state_raw.get("postgres_uri_env") or None,
    )

    delivery_raw = raw.get("delivery", {})
    # Retired with the byte-denominated flush controller (2026-08-28,
    # flush-sizing endgame): WARN-ignored, never refused — the replacement
    # rows controller is on by default, so a stale chart carrying these
    # silently gets the new defaults, which is the safe direction (contrast
    # the M4 poll-section refusal list, where a silent default would have
    # meant a deleted safety bound).
    for retired in (
        "flush_max_rows",
        "flush_max_bytes",
        "flush_adaptive_min_bytes",
        "flush_adaptive_step_bytes",
    ):
        if retired in delivery_raw:
            log.warning(
                "config: delivery.%s was removed with the byte-denominated flush controller "
                "(rows-denominated now — see flush-sizing-endgame-2026-08-28.md); ignoring it",
                retired,
            )
    delivery = DeliveryConfig(
        workers=_validate_int(delivery_raw.get("workers", 8), "delivery.workers"),
        flush_interval_seconds=float(delivery_raw.get("flush_interval_seconds", 120.0)),
        buffer_total_max_bytes=_validate_int(
            delivery_raw.get("buffer_total_max_bytes", 1_073_741_824), "delivery.buffer_total_max_bytes"
        ),
        buffer_max_bytes_per_destination=_validate_int(
            delivery_raw.get("buffer_max_bytes_per_destination", 0),
            "delivery.buffer_max_bytes_per_destination",
        ),
        flush_batch_max_rows=_validate_int(
            delivery_raw.get("flush_batch_max_rows", 60_000), "delivery.flush_batch_max_rows"
        ),
        flush_adaptive=bool(delivery_raw.get("flush_adaptive", True)),
        flush_adaptive_low_seconds=float(delivery_raw.get("flush_adaptive_low_seconds", 5.0)),
        flush_adaptive_high_seconds=float(delivery_raw.get("flush_adaptive_high_seconds", 30.0)),
        flush_adaptive_step_rows=_validate_int(
            delivery_raw.get("flush_adaptive_step_rows", 4_000), "delivery.flush_adaptive_step_rows"
        ),
        flush_adaptive_min_rows=_validate_int(
            delivery_raw.get("flush_adaptive_min_rows", 4_000), "delivery.flush_adaptive_min_rows"
        ),
        flush_adaptive_reprobe_after=_validate_int(
            delivery_raw.get("flush_adaptive_reprobe_after", 50), "delivery.flush_adaptive_reprobe_after"
        ),
        pool_max_open=_validate_int(delivery_raw.get("pool_max_open", 100), "delivery.pool_max_open"),
        flush_deadline_seconds=float(delivery_raw.get("flush_deadline_seconds", 0.0)),
        flush_circuit_failures=_validate_int(
            delivery_raw.get("flush_circuit_failures", 3), "delivery.flush_circuit_failures"
        ),
        flush_circuit_max_seconds=float(delivery_raw.get("flush_circuit_max_seconds", 900.0)),
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

    disc_raw = raw.get("discovery", {}) or {}
    try:
        discovery = DiscoveryConfig(
            enabled=bool(disc_raw.get("enabled", False)),
            url=disc_raw.get("url"),
            auth_header_name=disc_raw.get("auth_header_name"),
            auth_token_env=disc_raw.get("auth_token_env"),
            poll_interval_s=float(disc_raw.get("poll_interval_s", 60.0)),
            request_timeout_s=float(disc_raw.get("request_timeout_s", 10.0)),
            min_destinations=int(disc_raw.get("min_destinations", 1)),
            materialize_deadline_s=float(disc_raw.get("materialize_deadline_s", 60.0)),
            secret_cache_ttl_s=float(disc_raw.get("secret_cache_ttl_s", 300.0)),
            apply_enabled=bool(disc_raw.get("apply_enabled", False)),
            absent_stop_fetches=int(disc_raw.get("absent_stop_fetches", 3)),
            stop_floor_fraction=float(disc_raw.get("stop_floor_fraction", 0.5)),
            restart_min_interval_s=float(disc_raw.get("restart_min_interval_s", 300.0)),
            allowed_endpoint_suffixes=tuple(
                disc_raw.get("allowed_endpoint_suffixes", [".ducklings.svc", ".ducklings.svc.cluster.local"])
            ),
            allowed_secret_namespaces=tuple(disc_raw.get("allowed_secret_namespaces", ["ducklings"])),
            defaults=disc_raw.get("defaults", {}) or {},
        )
    except (TypeError, ValueError) as e:
        # An explicit `null` (or wrong type) on a numeric key raises a raw
        # TypeError from the coercion — surface it as config guidance.
        raise ConfigError(f"discovery section invalid: {e}") from e

    return ViaduckConfig(
        source=source,
        routing=routing,
        destinations=destinations,
        poll=poll,
        server=server,
        memory=memory,
        web=web,
        instance=instance,
        state=state,
        delivery=delivery,
        discovery=discovery,
    )
