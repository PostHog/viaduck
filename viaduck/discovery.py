"""CP-driven destination discovery (workstream C2/C5/C6 of dynamic sourcing).

Polls the duckgres control plane's read-only discovery endpoint
(``GET /api/v1/warehouses``, authenticated with the scoped read-only
secret) and maps (warehouse, team) pairs onto destination configs:

- id ``org-<org_id>-team-<team_id>``; routing value = the team id.
- **Table = the payload's ``events_table`` VERBATIM** — the CP owns
  naming (schema-per-team + legacy bare-name overrides resolve there);
  viaduck never derives a table name. Renames are not allowed upstream,
  so the table is immutable for a destination's lifetime.
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
- Warehouses in a non-writable state (resharding) are skipped with a
  metric: the write fence says nothing may write to them, and in a
  fixed-set world "skip at startup" is the only lever. C3 maps this
  onto the lifecycle machinery instead.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import urllib.request
from dataclasses import dataclass, field

from viaduck import metrics
from viaduck.config import DestinationConfig
from viaduck.k8s_secrets import SecretReadError, read_secret_key

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


def build_attach_uri(m: MappedDestination, password: str, sslmode: str) -> str:
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


def map_payload(payload: dict, *, count_broken: bool = True) -> list[MappedDestination]:
    """Map the payload onto destination candidates. Per-entry failures
    are counted and skipped; this never raises on data problems (a
    malformed warehouse degrades that warehouse, not the process).
    `count_broken=False` is the DriftWatcher's quiet mode — a warehouse
    stuck resharding for hours must not increment the materialization
    counter or WARN once per poll."""
    out: list[MappedDestination] = []
    for wh in payload.get("warehouses", []):
        try:
            _map_warehouse(wh, out, count_broken)
        except Exception as e:
            _broken("malformed", f"warehouse entry unparseable: {e!r}", count=count_broken)
    return out


def _map_warehouse(wh: dict, out: list[MappedDestination], count_broken: bool) -> None:
    org = wh.get("org_id", "?")
    if not wh.get("writable", False):
        _broken(
            "not_writable",
            f"org {org} state={wh.get('state')} (reshard fence; C3 maps this to lifecycle)",
            count=count_broken,
        )
        return
    bucket = wh.get("bucket")
    if not bucket:
        _broken("no_bucket", f"org {org} has no bucket in the payload", count=count_broken)
        return
    ms = wh.get("metadata_store") or {}
    ref = ms.get("password_secret_ref") or {}
    if not (ms.get("endpoint") and ms.get("database") and ms.get("username")):
        # cnpg rows carry only the store KIND until the provisioner
        # backfills connection details onto the row (duckgres
        # CLAUDE.md, Discovery Endpoints). Skip-and-count keeps this
        # forward-compatible with the backfill landing.
        _broken(
            "no_metadata_store",
            f"org {org} metadata_store missing endpoint/database/username",
            count=count_broken,
        )
        return
    if not (ref.get("namespace") and ref.get("name") and ref.get("key")):
        _broken("no_secret_ref", f"org {org} metadata_store has no usable password_secret_ref", count=count_broken)
        return
    for team in wh.get("teams", []):
        team_id = team.get("team_id")
        events_table = team.get("events_table")
        if team_id is None or not events_table:
            _broken("bad_team_row", f"org {org} team row missing team_id/events_table", count=count_broken)
            continue
        # `enabled` is the QUERY-SERVING switch, deliberately ignored:
        # row presence is the only ingestion signal (duckgres
        # migration 000024 contract — deriving ingestion-stop from a
        # serving hold would turn it into permanent event loss).
        out.append(
            MappedDestination(
                dest_id=f"org-{org}-team-{team_id}",
                org_id=org,
                team_id=team_id,
                table=events_table,
                data_path=f"s3://{bucket}/",
                pg_endpoint=ms["endpoint"],
                pg_port=ms.get("port") or 5432,
                pg_database=ms["database"],
                pg_username=ms["username"],
                secret_namespace=ref["namespace"],
                secret_name=ref["name"],
                secret_key=ref["key"],
            )
        )


def materialize(
    mapped: list[MappedDestination],
    static_routing_values: set[str],
    defaults: dict,
    static_ids: set[str] | None = None,
    *,
    deadline_s: float = 60.0,
    heartbeat=None,
    secret_timeout_s: float = 10.0,
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
        if rv in static_routing_values:
            log.warning(
                "Discovered destination %s: routing value %s owned by a static destination — static wins "
                "(delete the static entry to cut this tenant over to discovery; gap-free cutover requires "
                "the static id to already equal %s so the cursor row carries over)",
                m.dest_id,
                rv,
                m.dest_id,
            )
            continue
        if m.dest_id in static_ids:
            _broken("id_collision", f"{m.dest_id} collides with a static destination id (different routing value)")
            continue
        # Defense-in-depth against a spoofed payload (it directs which
        # Secret we read and where the password gets sent in a libpq
        # handshake): endpoints and secret namespaces must match the
        # configured allowlists.
        if allowed_endpoint_suffixes and not any(m.pg_endpoint.endswith(sfx) for sfx in allowed_endpoint_suffixes):
            _broken("endpoint_not_allowed", f"{m.dest_id}: endpoint {m.pg_endpoint!r} outside allowed suffixes")
            continue
        if allowed_secret_namespaces and m.secret_namespace not in allowed_secret_namespaces:
            _broken("namespace_not_allowed", f"{m.dest_id}: secret namespace {m.secret_namespace!r} not allowed")
            continue
        if rv in seen_rvs or m.dest_id in seen_ids:
            _broken("duplicate", f"{m.dest_id}: payload served routing value {rv} or id more than once (first wins)")
            continue
        try:
            password = read_secret_key(m.secret_namespace, m.secret_name, m.secret_key, timeout_s=secret_timeout_s)
        except SecretReadError as e:
            _broken("secret_unreadable", f"{m.dest_id}: {e}")
            continue
        seen_rvs.add(rv)
        seen_ids.add(m.dest_id)
        uri = build_attach_uri(m, password, defaults.get("sslmode", "disable"))
        props = {"memory_limit": defaults.get("memory_limit", "8GB")}
        props.update(defaults.get("properties", {}))
        out.append(
            DestinationConfig(
                id=m.dest_id,
                routing_value=rv,
                name=m.dest_id,
                postgres_uri_env="",
                postgres_uri_direct=uri,
                data_path=m.data_path,
                table=m.table,
                properties=props,
                # Canonical shape is uniform across discovered tenants:
                # projection on, captured_at dropped (plan C2 convention
                # defaults; per-org exceptions stay static-config-only).
                schema_projection_enabled=True,
                drop_source_columns=tuple(defaults.get("drop_source_columns", ("captured_at",))),
            )
        )
    return out


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
    _stop: threading.Event = field(default_factory=threading.Event)
    _thread: threading.Thread | None = None
    _last_drift: tuple[frozenset, frozenset, frozenset] = (frozenset(), frozenset(), frozenset())
    _unwritable_reported: set[str] = field(default_factory=set)
    _recovery_logged: bool = False

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
        current = {m.dest_id: m for m in map_payload(payload, count_broken=False)}
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
