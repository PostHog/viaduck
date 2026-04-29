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
import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml


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


@dataclass(frozen=True)
class RoutingConfig:
    field: str
    key_columns: list[str] = field(default_factory=list)
    seed_mode: str = "scan"  # "scan" or "cdc_replay"

    def __post_init__(self):
        if self.seed_mode not in ("scan", "cdc_replay"):
            raise ConfigError(f"routing.seed_mode must be 'scan' or 'cdc_replay', got {self.seed_mode!r}")


@dataclass(frozen=True)
class DestinationConfig:
    id: str
    routing_value: str
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


@dataclass(frozen=True)
class PollConfig:
    interval_seconds: float = 5.0


@dataclass(frozen=True)
class ServerConfig:
    port: int = 8000


@dataclass(frozen=True)
class WebConfig:
    enabled: bool = True
    port: int = 8001


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


@dataclass(frozen=True)
class StateConfig:
    table: str = "_viaduck_state"


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
        key_columns=raw_key_cols,
        seed_mode=rt.get("seed_mode", "scan"),
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
            )
        )

    # Optional sections
    poll_raw = raw.get("poll", {})
    poll = PollConfig(interval_seconds=poll_raw.get("interval_seconds", 5.0))

    server_raw = raw.get("server", {})
    server = ServerConfig(port=server_raw.get("port", 8000))

    web_raw = raw.get("web", {})
    web = WebConfig(
        enabled=web_raw.get("enabled", True),
        port=web_raw.get("port", 8001),
    )

    state_raw = raw.get("state", {})
    state = StateConfig(table=state_raw.get("table", "_viaduck_state"))

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
    )
