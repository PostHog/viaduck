"""CP-driven destination discovery (workstream C2/C5/C6 of dynamic sourcing).

Polls the duckgres control plane's read-only discovery endpoint
(``GET /api/v1/warehouses``, authenticated with the scoped read-only
secret) and maps (warehouse, team) pairs onto destination configs:

- id ``org-<org_id>-team-<team_id>``; routing value = the team id.
- **Table = a payload table field VERBATIM** — ``events_table`` by
  default; a persons pipeline sets ``discovery.table_field`` to
  ``persons_table``/``persons_distinct_ids_table``. The CP owns naming
  (schema-per-team + legacy bare-name overrides resolve there); viaduck
  never derives a table name. Renames are not allowed upstream, so the
  table is immutable for a destination's lifetime.
- Metadata-store credentials come from the payload's connection fields
  plus a Kubernetes Secret reference (``password_secret_ref``) resolved
  by reading the Secret directly with viaduck's ServiceAccount (RBAC
  into the tenant namespace — no per-tenant secret copies, no plaintext
  in the payload).
- Discovered destinations default to ``seed_mode`` **latest** semantics:
  discovery starts the stream; it never backfills (that stays with
  provisioning/DLT).

M4 semantics — ADDITIVE, STATIC WINS, FIXED SET:

- The destination set is still fixed at process startup. Discovery runs
  once during startup to extend the static set; the background poller
  only DETECTS drift (new/removed/changed entries vs what this process
  materialized) and surfaces it via metrics + WARN logs. Applying drift
  is a restart (the runtime-mutable set is workstream C3).
- A static destination wins any routing-value collision with a
  discovered one (the C6 migration contract: cutover per tenant =
  delete the static entry, the discovered twin takes over).
- Fail-open at startup: if the CP is unreachable, viaduck starts with
  the static set and a loud signal (``viaduck_discovery_synced`` = 0)
  rather than holding static tenants hostage to CP availability. The
  durable source makes late pickup safe; the alert makes it visible.
- Fail-safe per entry: a warehouse/team row that cannot be materialized
  (missing bucket, empty metadata-store connection fields, unreadable
  secret) is skipped and counted on
  ``viaduck_discovery_broken_entries_total{reason}`` — one broken
  tenant must not take down discovery for the rest.
- Warehouses in a non-writable state (resharding) are not startable:
  their teams stay MENTIONED (never absent) but get no config. During a
  reshard viaduck simply keeps failing against the DB-side NOLOGIN
  fence until the changed config arrives (C3 v6 — rare, fast,
  operationally routine).
"""

from __future__ import annotations

import json
import logging
import threading
import time
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from viaduck import metrics
from viaduck.config import DeferredUriSource, DestinationConfig
from viaduck.k8s_secrets import SecretReadError, read_secret_key_cached, read_secret_key_fresh
from viaduck.scrub import scrub_credentials

log = logging.getLogger(__name__)

# Cap on the endpoint response body — same rationale as millpond's
# include-values reader: the largest plausible payload is far below this;
# anything bigger is a misconfigured URL and an unbounded read is an OOM.
_MAX_RESPONSE_BYTES = 8 * 1024 * 1024


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Refuse redirects: urllib re-sends custom headers (the read-only
    token) to the redirect target, including cross-host."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


_OPENER = urllib.request.build_opener(_NoRedirect())


class DiscoveryError(Exception):
    """Fetch/parse-level failure — the whole poll failed (vs per-entry
    skips, which are counted and never raise)."""


def fetch(url: str, auth_header: tuple[str, str] | None, timeout_s: float) -> dict:
    """Fetch and parse the discovery payload. Raises DiscoveryError on any
    transport/shape problem — the caller keeps its last-known state."""
    req = urllib.request.Request(url)
    if auth_header is not None:
        req.add_header(*auth_header)
    try:
        with _OPENER.open(req, timeout=timeout_s) as resp:
            body = resp.read(_MAX_RESPONSE_BYTES + 1)
    except Exception as e:
        raise DiscoveryError(f"discovery fetch failed: {e}") from e
    if len(body) > _MAX_RESPONSE_BYTES:
        raise DiscoveryError(f"discovery response exceeds {_MAX_RESPONSE_BYTES} bytes")
    try:
        payload = json.loads(body)
    except ValueError as e:
        raise DiscoveryError(f"discovery response is not JSON: {e}") from e
    if not isinstance(payload, dict) or "warehouses" not in payload or "config_generation" not in payload:
        raise DiscoveryError("discovery response missing warehouses/config_generation")
    if not isinstance(payload["warehouses"], list) or not all(isinstance(w, dict) for w in payload["warehouses"]):
        raise DiscoveryError("discovery warehouses is not a list of objects")
    if isinstance(payload["config_generation"], bool) or not isinstance(payload["config_generation"], (int, float)):
        raise DiscoveryError("discovery config_generation is not numeric")
    return payload


@dataclass(frozen=True)
class MappedDestination:
    """A discovered destination plus the metadata needed to build its
    config. Credential resolution is deferred to materialization so the
    mapping layer stays pure and testable."""

    dest_id: str
    org_id: str
    team_id: int
    table: str
    data_path: str
    pg_endpoint: str
    pg_port: int
    pg_database: str
    pg_username: str
    secret_namespace: str
    secret_name: str
    secret_key: str


def _broken(reason: str, detail: str, *, count: bool = True) -> None:
    # Structural no-secrets-in-logs: detail strings can derive from
    # exception text raised near secret handling (SecretReadError et al
    # are DESIGNED secret-free, but scrubbing at the boundary makes that
    # a property instead of a promise).
    detail = scrub_credentials(detail)
    if count:
        metrics.discovery_broken_entries_total.labels(reason=reason).inc()
        log.warning("Discovery entry skipped (%s): %s", reason, detail)
    else:
        # Drift-poll quiet mode: visible at DEBUG, no counter/WARN churn.
        log.debug("Discovery entry skipped (%s, drift poll): %s", reason, detail)


def _libpq_quote(value: str) -> str:
    """Quote a libpq keyword-form value: wrap in single quotes with
    backslash-escaping. Always quoting is simpler than deciding when it's
    needed and is accepted by libpq for every value."""
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def build_attach_uri(m: MappedDestination | DeferredUriSource, password: str, sslmode: str) -> str:
    """DuckLake ATTACH connection string. TWO parsing layers stack here
    and each has bitten a review round:

    1. pyducklake embeds this string RAW inside a SQL literal
       (``ATTACH 'ducklake:{uri}' ...``), so every single quote in it
       must be SQL-doubled or the first ``'`` closes the outer literal
       and DuckDB dies with a ParserException before any connection is
       attempted (round-2 FATAL).
    2. DuckLake's postgres catalog needs the libpq KEYWORD form with the
       ``postgres:`` prefix — a ``postgresql://`` URL falls through to
       the FILE backend (round-1 FATAL; also the config.py docstring).

    So: libpq-quote each value (single quotes + backslash escaping, which
    makes spaces/quotes in passwords safe for libpq), then SQL-double
    every quote in the final string. Empirically validated end-to-end
    against a live Postgres with quote/space/backslash passwords.
    test_runtime attach-parse test guards layer 1; never assert this
    function's output against a hand-written string without running it
    through a real ATTACH first."""
    parts = {
        "host": m.pg_endpoint,
        "port": m.pg_port,
        "user": m.pg_username,
        "password": password,
        "dbname": m.pg_database,
        "sslmode": sslmode,
    }
    conninfo = "postgres:" + " ".join(f"{k}={_libpq_quote(v)}" for k, v in parts.items())
    return conninfo.replace("'", "''")


# Per-destination classification of the RAW payload (C3 §4). At the
# mapped level a fenced/broken tenant is indistinguishable from an
# absent one (map_payload historically returned before emitting rows for
# unwritable orgs). The consumer model is TWO predicates (C3 v6):
# MENTIONED (the id appears in the payload at all — never absent, no
# matter why it isn't startable; the WHY lives in the existing
# discovery_broken_entries_total{reason} counters) and STARTABLE
# (mentioned with a complete, usable config — `mapped` is populated).
# ABSENT is deliberately NOT a payload property: it is derived by the
# consumer via derive_absent() (registry-minus-view).


@dataclass(frozen=True)
class ClassifiedEntry:
    dest_id: str
    mapped: MappedDestination | None  # populated iff startable

    @property
    def startable(self) -> bool:
        return self.mapped is not None


@dataclass(frozen=True)
class ClassifiedView:
    """Immutable per-fetch view the DriftWatcher publishes and the
    reconciler consumes.

    ``parse_poisoned`` — some payload content failed to parse into
    NAMEABLE destination ids (unparseable warehouse entry, malformed
    team row). Those ids would otherwise read as ABSENT and tick stop
    debounces; a poisoned view freezes absence evaluation for the WHOLE
    view (fetch-failed-for-absence-purposes-only) while adds/changes
    from parseable entries still apply (C3 §4, v4 review QE F7).

    ``generation`` is the payload's config_generation — an opaque change
    token, compare for equality only; staleness detection is by view
    OBJECT identity, never generation equality (generations legitimately
    repeat)."""

    generation: float
    fetched_at: float
    entry_list: tuple[ClassifiedEntry, ...]
    parse_poisoned: bool
    entries: Mapping[str, ClassifiedEntry] = field(init=False)

    def __post_init__(self) -> None:
        # Dedupe on duplicate ids (a CP bug serving one team twice):
        # first STARTABLE occurrence wins, falling back to first
        # occurrence — exactly materialize()'s dedupe over the
        # startable-only list, so stage-4 rule 1 and startup can never
        # disagree on a mixed-startability duplicate (F7, stage-3
        # review). entry_list preserves duplicates so map_payload/
        # materialize keep their existing duplicate counting.
        first: dict[str, ClassifiedEntry] = {}
        for e in self.entry_list:
            held = first.get(e.dest_id)
            if held is None or (not held.startable and e.startable):
                first[e.dest_id] = e
        object.__setattr__(self, "entries", MappingProxyType(first))


def classify_payload(payload: dict, *, count_broken: bool = True, table_field: str = "events_table") -> ClassifiedView:
    """Classify every enumerable destination id in the raw payload as
    startable (mapped config) or merely mentioned (fenced, degraded —
    the reason is counted, not carried). Never raises on data problems:
    un-enumerable content poisons the view (see ClassifiedView).
    `count_broken` keeps the startup/loud vs drift-poll/quiet split of
    map_payload. `table_field` names the team-payload field read as the
    destination table (see DiscoveryConfig.table_field)."""
    entries: list[ClassifiedEntry] = []
    poisoned = False
    for wh in payload.get("warehouses", []):
        try:
            if not _classify_warehouse(wh, entries, count_broken, table_field):
                poisoned = True
        except Exception as e:
            _broken("malformed", f"warehouse entry unparseable: {e!r}", count=count_broken)
            poisoned = True
    return ClassifiedView(
        # fetch() validates the key for every production payload; the .get
        # keeps classify/map_payload's never-raise-on-data-problems
        # contract for direct callers (F5, stage-3 review).
        generation=payload.get("config_generation", -1),
        fetched_at=time.monotonic(),
        entry_list=tuple(entries),
        parse_poisoned=poisoned,
    )


def derive_absent(view: ClassifiedView | None, registry_snapshot) -> frozenset[str]:
    """Discovered-origin routable ids the view does not mention — the
    ONLY correct ABSENT derivation (C3 §4):

    - statics are never in the CP view and must never enter absence
      evaluation (their exclusion here is what the scoping line in the
      design means mechanically);
    - a poisoned or missing view yields NO absences (freeze, don't stop
      on unattributable parse failures);
    - a MENTIONED destination is never absent, startable or not — a
      fenced or degraded tenant must not tick a stop debounce.
    """
    if view is None or view.parse_poisoned:
        return frozenset()
    discovered = registry_snapshot.discovered_ids()
    routable_discovered = discovered & registry_snapshot.routable_ids
    return frozenset(d for d in routable_discovered if d not in view.entries)


def map_payload(
    payload: dict, *, count_broken: bool = True, table_field: str = "events_table"
) -> list[MappedDestination]:
    """STARTABLE entries of the classified view, in payload order with
    duplicates preserved (materialize() owns dedupe + its counter).
    Startup-compatible shape; the classification is the single parsing
    path so the two can never drift."""
    view = classify_payload(payload, count_broken=count_broken, table_field=table_field)
    return [e.mapped for e in view.entry_list if e.mapped is not None]


def _classify_warehouse(wh: dict, entries: list[ClassifiedEntry], count_broken: bool, table_field: str) -> bool:
    """Classify one warehouse's teams into `entries`. Returns False when
    any content was UN-ENUMERABLE (a team id we cannot even name — that
    id would falsely read as ABSENT, so the caller poisons the view).
    Everything nameable is MENTIONED; it is additionally STARTABLE when
    the warehouse is writable and its config is complete. The WHY of a
    mentioned-but-unstartable tenant (reshard fence, missing bucket,
    partial metadata store, no secret ref) lives in the _broken
    counters/logs, which keep map_payload's historical reasons and
    cadence — the consumer doesn't branch on it (C3 v6: during a
    reshard we simply keep failing against the DB-side fence; rare,
    fast, operationally routine)."""
    org = wh.get("org_id", "?")
    teams = wh.get("teams", [])
    enumerable = True

    # Startability: writable + bucket + complete metadata store + secret
    # ref. The first failed check counts the warehouse-level reason once
    # (historical cadence) and makes every team mentioned-only.
    startable = True
    bucket = wh.get("bucket")
    ms = wh.get("metadata_store") or {}
    ref = ms.get("password_secret_ref") or {}
    if not wh.get("writable", False):
        # Reshard fence: the warehouse (and its teams) stays in the
        # payload throughout the operation (duckgres #1005 runbook
        # contract), so its teams stay MENTIONED — never absent.
        _broken(
            "not_writable",
            f"org {org} state={wh.get('state')} (reshard fence; flushes fail against the DB fence until it lifts)",
            count=count_broken,
        )
        startable = False
    elif not bucket:
        _broken("no_bucket", f"org {org} has no bucket in the payload", count=count_broken)
        startable = False
    elif not (ms.get("endpoint") and ms.get("database") and ms.get("username")):
        # cnpg rows carry only the store KIND until the provisioner
        # backfills connection details onto the row (duckgres
        # CLAUDE.md, Discovery Endpoints). Skip-and-count keeps this
        # forward-compatible with the backfill landing.
        _broken(
            "no_metadata_store",
            f"org {org} metadata_store missing endpoint/database/username",
            count=count_broken,
        )
        startable = False
    elif not (ref.get("namespace") and ref.get("name") and ref.get("key")):
        _broken("no_secret_ref", f"org {org} metadata_store has no usable password_secret_ref", count=count_broken)
        startable = False

    for team in teams:
        team_id = team.get("team_id")
        table = team.get(table_field)
        if team_id is None:
            # An id we cannot name — the poison case. Deliberate cadence
            # change vs the pre-v6 parser: this fires for unstartable
            # warehouses too (the old code early-returned before the team
            # loop), because the row must be scanned to poison — the id
            # is unnameable regardless of fence state.
            _broken("bad_team_row", f"org {org} team row missing team_id", count=count_broken)
            enumerable = False
            continue
        dest_id = f"org-{org}-team-{team_id}"
        if startable and not table:
            _broken("bad_team_row", f"org {org} team row missing {table_field}", count=count_broken)
            entries.append(ClassifiedEntry(dest_id=dest_id, mapped=None))
            continue
        if not startable:
            entries.append(ClassifiedEntry(dest_id=dest_id, mapped=None))
            continue
        # `enabled` is the QUERY-SERVING switch, deliberately ignored:
        # row presence is the only ingestion signal (duckgres
        # migration 000024 contract — deriving ingestion-stop from a
        # serving hold would turn it into permanent event loss).
        entries.append(
            ClassifiedEntry(
                dest_id=dest_id,
                mapped=MappedDestination(
                    dest_id=dest_id,
                    org_id=org,
                    team_id=team_id,
                    table=table,
                    data_path=f"s3://{bucket}/",
                    pg_endpoint=ms["endpoint"],
                    pg_port=ms.get("port") or 5432,
                    pg_database=ms["database"],
                    pg_username=ms["username"],
                    secret_namespace=ref["namespace"],
                    secret_name=ref["name"],
                    secret_key=ref["key"],
                ),
            )
        )
    return enumerable


def materialize(
    mapped: list[MappedDestination],
    static_routing_values: set[str],
    defaults: dict,
    static_ids: set[str] | None = None,
    *,
    deadline_s: float = 60.0,
    heartbeat=None,
    secret_timeout_s: float = 10.0,
    secret_cache_ttl_s: float = 300.0,
    allowed_endpoint_suffixes: tuple[str, ...] = (),
    allowed_secret_namespaces: tuple[str, ...] = (),
) -> list[DestinationConfig]:
    """Turn mapped candidates into DestinationConfigs, resolving each
    password Secret via the ServiceAccount. STATIC WINS: a candidate whose
    routing value OR id collides with a static destination is dropped with
    a WARN (the C6 cutover contract). Duplicates WITHIN the payload are
    deduped first-wins — collisions must degrade the entry, never reach
    ViaduckConfig.__post_init__ and crash startup (the fail-open promise
    covers static tenants). Secret failures skip the entry."""
    out: list[DestinationConfig] = []
    seen_rvs: set[str] = set()
    seen_ids: set[str] = set()
    static_ids = static_ids or set()
    deadline = time.monotonic() + deadline_s
    for m in mapped:
        if time.monotonic() > deadline:
            # Bounded startup: N sequential Secret reads against a
            # blackholed API server must not exceed the liveness grace
            # and crashloop static tenants. Remaining entries degrade
            # like any other broken entry.
            _broken("deadline", f"{m.dest_id}: materialization deadline ({deadline_s}s) exceeded; skipping the rest")
            break
        if heartbeat is not None:
            heartbeat()
        rv = str(m.team_id)
        if rv in seen_rvs or m.dest_id in seen_ids:
            _broken("duplicate", f"{m.dest_id}: payload served routing value {rv} or id more than once (first wins)")
            continue
        cfg = materialize_one(
            m,
            static_routing_values,
            defaults,
            static_ids,
            secret_timeout_s=secret_timeout_s,
            secret_cache_ttl_s=secret_cache_ttl_s,
            allowed_endpoint_suffixes=allowed_endpoint_suffixes,
            allowed_secret_namespaces=allowed_secret_namespaces,
        )
        if cfg is None:
            continue
        seen_rvs.add(rv)
        seen_ids.add(m.dest_id)
        out.append(cfg)
    return out


def materialize_one(
    m: MappedDestination,
    static_routing_values: set[str],
    defaults: dict,
    static_ids: set[str] | None = None,
    *,
    secret_timeout_s: float = 10.0,
    secret_cache_ttl_s: float = 300.0,
    allowed_endpoint_suffixes: tuple[str, ...] = (),
    allowed_secret_namespaces: tuple[str, ...] = (),
    probe_fresh: bool = False,
) -> DestinationConfig | None:
    """One candidate -> DestinationConfig, or None (reason counted).
    Shared by startup materialize() and the reconciler's activate
    (which passes probe_fresh=True: an activation must prove the secret
    is readable NOW — no TTL hit, no stale-fallback)."""
    static_ids = static_ids or set()
    rv = str(m.team_id)
    if rv in static_routing_values:
        log.warning(
            "Discovered destination %s: routing value %s owned by a static destination — static wins "
            "(delete the static entry to cut this tenant over to discovery; gap-free cutover requires "
            "the static id to already equal %s so the cursor row carries over)",
            m.dest_id,
            rv,
            m.dest_id,
        )
        return None
    if m.dest_id in static_ids:
        _broken("id_collision", f"{m.dest_id} collides with a static destination id (different routing value)")
        return None
    # Defense-in-depth against a spoofed payload (it directs which
    # Secret we read and where the password gets sent in a libpq
    # handshake): endpoints and secret namespaces must match the
    # configured allowlists.
    if allowed_endpoint_suffixes and not any(m.pg_endpoint.endswith(sfx) for sfx in allowed_endpoint_suffixes):
        _broken("endpoint_not_allowed", f"{m.dest_id}: endpoint {m.pg_endpoint!r} outside allowed suffixes")
        return None
    if allowed_secret_namespaces and m.secret_namespace not in allowed_secret_namespaces:
        _broken("namespace_not_allowed", f"{m.dest_id}: secret namespace {m.secret_namespace!r} not allowed")
        return None
    try:
        # Materializability PROBE (C3 §4/§5): the password is NOT baked
        # into the config — the pool resolves the ref at
        # connection-create time (TTL cache, stale-fallback), so
        # rotation heals via ordinary evict/recreate. The probe keeps
        # RBAC/absence validation loud (secret_unreadable counter) and
        # warms the cache so the first flush does no API read. Startup
        # probes through the TTL cache (cold at process start — a real
        # read); the reconciler's activate passes probe_fresh=True
        # ("validate now": always hits the API, never stale-falls-back).
        if probe_fresh:
            read_secret_key_fresh(m.secret_namespace, m.secret_name, m.secret_key, timeout_s=secret_timeout_s)
        else:
            read_secret_key_cached(
                m.secret_namespace,
                m.secret_name,
                m.secret_key,
                ttl_s=secret_cache_ttl_s,
                timeout_s=secret_timeout_s,
            )
    except SecretReadError as e:
        _broken("secret_unreadable", f"{m.dest_id}: {e}")
        return None
    props = {"memory_limit": defaults.get("memory_limit", "8GB")}
    props.update(defaults.get("properties", {}))
    return DestinationConfig(
        id=m.dest_id,
        routing_value=rv,
        name=m.dest_id,
        postgres_uri_env="",
        uri_source=DeferredUriSource(
            pg_endpoint=m.pg_endpoint,
            pg_port=m.pg_port,
            pg_database=m.pg_database,
            pg_username=m.pg_username,
            secret_namespace=m.secret_namespace,
            secret_name=m.secret_name,
            secret_key=m.secret_key,
            sslmode=defaults.get("sslmode", "disable"),
        ),
        data_path=m.data_path,
        table=m.table,
        properties=props,
        # Canonical shape is uniform across discovered tenants:
        # projection on, captured_at dropped (plan C2 convention
        # defaults; per-org exceptions stay static-config-only).
        schema_projection_enabled=True,
        drop_source_columns=tuple(defaults.get("drop_source_columns", ("captured_at",))),
    )


@dataclass
class DriftWatcher:
    """Background poller (M4): re-fetches the payload and compares the
    MAPPED view (same level as its baseline — comparing against the
    materialized set would report permanent false "added" drift for the
    expected static-wins pre-cutover state) against what this process saw
    at startup. Three drift kinds:

    - added/removed: id-set differences (restart to apply);
    - changed: same id, different content — MappedDestination is a frozen
      dataclass, so equality is structural. This is the reshard tell: a
      completed reshard brings the id back with a NEW metadata endpoint,
      which an id-set comparison reads as "no drift" while this process
      keeps writing the OLD store (split-brain if it is still alive).

    A startup org turning not-writable is escalated to ERROR — that is
    the reshard write fence starting, and in a fixed-set world the
    operator must pause (lifecycle) or restart; see the README reshard
    note. All drift is DETECT-ONLY: the watcher never mutates the set.
    Transitions are also counted (drift_transitions_total) because
    60s-poll gauge flaps are unalertable.
    """

    url: str
    auth_header: tuple[str, str] | None
    timeout_s: float
    poll_interval_s: float
    baseline: dict[str, MappedDestination]
    startup_generation: int
    # Team-payload field read as the destination table — must match the
    # field the startup map_payload used, or every discovered entry reads
    # as changed drift.
    table_field: str = "events_table"
    # True when the C3 reconciler is applying views (discovery.
    # apply_enabled): the vs-STARTUP drift comparison below is then
    # permanently wrong after the first applied change (an applied add
    # reads as drift forever, and "restart to apply" is false guidance),
    # so it is skipped — pending UNAPPLIED work is exported by the
    # reconciler on viaduck_reconciler_pending{reason} instead. The
    # applied-registry-baseline rework lands with the dashboards PR.
    apply_mode: bool = False
    _stop: threading.Event = field(default_factory=threading.Event)
    _thread: threading.Thread | None = None
    _last_drift: tuple[frozenset, frozenset, frozenset] = (frozenset(), frozenset(), frozenset())
    _unwritable_reported: set[str] = field(default_factory=set)
    _recovery_logged: bool = False
    _poison_reported: bool = False
    # Published classified view (C3 stage 3): replaced WHOLE per
    # successful fetch; a failed fetch publishes nothing (the consumer
    # detects staleness by object identity — same view object means
    # frozen counters). The reconciler (stage 4) pulls latest() once per
    # poll cycle; until then this is detect-only output.
    _view: ClassifiedView | None = None
    _view_lock: threading.Lock = field(default_factory=threading.Lock)

    def latest(self) -> ClassifiedView | None:
        """Most recent classified view, or None before the first
        successful poll (incl. a failed-startup process — the reconciler
        treats None as no-view: nothing fires)."""
        with self._view_lock:
            return self._view

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="discovery-drift", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)

    def _run(self) -> None:
        while not self._stop.is_set():
            self._stop.wait(self.poll_interval_s)
            if self._stop.is_set():
                return
            try:
                self._poll_once()
            except Exception:
                # The thread must never die silently — a dead watcher with
                # frozen gauges reads as "no drift", which is worse than
                # no watcher at all. synced is startup-owned and untouched
                # here; poll health has its own failure counter and the
                # last-success timestamp ages visibly.
                metrics.discovery_poll_failures_total.inc()
                log.warning("Discovery drift poll failed (keeping last view)", exc_info=True)

    def _poll_once(self) -> None:
        payload = fetch(self.url, self.auth_header, self.timeout_s)
        # Deliberately NOT record_success_metrics: synced=1 means "this
        # process reflects the CP" and only the startup path may say so —
        # a drift-poll success after a failed startup must not clear the
        # static-only alert (round-2 review).
        metrics.discovery_config_generation.set(payload["config_generation"])
        metrics.discovery_last_success_timestamp_seconds.set_to_current_time()
        view = classify_payload(payload, count_broken=False, table_field=self.table_field)
        with self._view_lock:
            self._view = view
        startable = sum(1 for e in view.entries.values() if e.startable)
        metrics.discovery_classified.labels(classification="startable").set(startable)
        metrics.discovery_classified.labels(classification="mentioned_only").set(len(view.entries) - startable)
        metrics.discovery_view_poisoned.set(1 if view.parse_poisoned else 0)
        if view.parse_poisoned and not self._poison_reported:
            self._poison_reported = True
            log.warning(
                "Discovery view is POISONED (un-nameable payload content, e.g. a team row missing "
                "team_id): absence evaluation is frozen for every view until the CP data is fixed — "
                "deprovisioned tenants will NOT be stopped while this persists (alert on "
                "viaduck_discovery_view_poisoned)"
            )
        elif not view.parse_poisoned and self._poison_reported:
            self._poison_reported = False
            log.info("Discovery view poison cleared; absence evaluation resumes")
        # Baseline drift comparison (skipped in apply_mode below) stays
        # on the startable/mapped level — last-wins dict over the raw
        # list, same construction as always.
        current = {e.dest_id: e.mapped for e in view.entry_list if e.mapped is not None}
        if self.apply_mode:
            # Reconciler active: the classified view IS the handoff; the
            # vs-startup comparison below would read every applied change
            # as permanent drift. The fence ERROR escalation is also
            # obsolete (no fixed set to protect — flushes fail against
            # the DB fence and rule 2 restarts on the changed config).
            return
        if not self.baseline and self.startup_generation == -1 and current and not self._recovery_logged:
            self._recovery_logged = True
            log.warning(
                "Discovery recovered after a failed startup: the CP now serves %d destination(s) "
                "this process is NOT delivering to — restart viaduck to materialize them",
                len(current),
            )

        baseline_ids = set(self.baseline)
        current_ids = set(current)
        added = frozenset(current_ids - baseline_ids)
        removed = frozenset(baseline_ids - current_ids)
        changed = frozenset(i for i in baseline_ids & current_ids if current[i] != self.baseline[i])
        metrics.discovery_drift_destinations.labels(kind="added").set(len(added))
        metrics.discovery_drift_destinations.labels(kind="removed").set(len(removed))
        metrics.discovery_drift_destinations.labels(kind="changed").set(len(changed))

        # Reshard fence signal: a baseline org now serving writable=false.
        baseline_orgs = {m.org_id for m in self.baseline.values()}
        for wh in payload["warehouses"]:
            org = wh.get("org_id")
            if org in baseline_orgs and not wh.get("writable", False):
                if org not in self._unwritable_reported:
                    self._unwritable_reported.add(org)
                    metrics.discovery_drift_transitions_total.labels(kind="unwritable").inc()
                    log.error(
                        "Org %s is no longer writable (reshard fence) but this process holds its "
                        "destinations in the fixed set — pause them via the lifecycle table NOW (or "
                        "restart after the reshard). Writes between fence-start and the pause may land "
                        "on the old store and be lost at cutover; pause latency is the exposure window",
                        org,
                    )
            elif org in self._unwritable_reported and wh.get("writable", False):
                self._unwritable_reported.discard(org)
                log.warning("Org %s is writable again — restart to pick up its (possibly moved) metadata store", org)

        new_drift = (added, removed, changed)
        if new_drift != self._last_drift:
            for kind, ids in (("added", added), ("removed", removed), ("changed", changed)):
                prev = self._last_drift[("added", "removed", "changed").index(kind)]
                if ids - prev:
                    metrics.discovery_drift_transitions_total.labels(kind=kind).inc()
            self._last_drift = new_drift
            if added or removed or changed:
                log.warning(
                    "Discovery drift vs startup (generation %s -> %s): +%d %s, -%d %s, ~%d changed %s — "
                    "restart viaduck to apply (a CHANGED id means its endpoint/bucket/table moved; "
                    "keeping the old view risks writing a stale metadata store)",
                    self.startup_generation,
                    payload["config_generation"],
                    len(added),
                    sorted(added)[:5],
                    len(removed),
                    sorted(removed)[:5],
                    len(changed),
                    sorted(changed)[:5],
                )
            else:
                log.info("Discovery drift cleared (generation %s)", payload["config_generation"])


def record_success_metrics(payload: dict) -> None:
    """Shared by the startup fetch and the drift poller."""
    metrics.discovery_synced.set(1)
    metrics.discovery_config_generation.set(payload["config_generation"])
    metrics.discovery_last_success_timestamp_seconds.set_to_current_time()
