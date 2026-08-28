from prometheus_client import Counter, Gauge, Histogram


class _AutoPipelineLabels:
    """Wrapper that auto-injects the pipeline label into .labels() calls."""

    def __init__(self, metric, pipeline: str):
        self._metric = metric
        self._pipeline = pipeline

    def labels(self, **kwargs):
        return self._metric.labels(pipeline=self._pipeline, **kwargs)

    def remove(self, *label_values):
        """Remove one child series (label values AFTER the auto-injected
        pipeline, in declaration order). Raises KeyError for an absent
        child — callers treat removal as best-effort."""
        self._metric.remove(self._pipeline, *label_values)


# --- Raw metric definitions (with pipeline as first label) ---

_polls_total = Counter(
    "viaduck_polls_total",
    "Poll cycles executed",
    ["pipeline"],
)
_self_recycles_total = Counter(
    "viaduck_self_recycles_total",
    "Clean watermark-triggered restarts (drain + exit 0 on RSS watermark)",
    ["pipeline"],
)
_cdc_read_seconds = Histogram(
    "viaduck_cdc_read_seconds",
    "Time to read CDC insertions from source",
    ["pipeline"],
)
_cdc_rows_read_total = Counter(
    "viaduck_cdc_rows_read_total",
    "Total rows read from source via CDC",
    ["pipeline"],
)
# Direct-SQL feed (unconditional for append_only; log-consumer-proposal.md §6.1).
# surface: catalog (psycopg metadata queries) vs parquet (duckdb data plane) —
# the split that makes the extension bypass measurable.
_cdc_feed_query_seconds = Histogram(
    "viaduck_cdc_feed_query_seconds",
    "Direct-SQL feed read latency by surface",
    ["pipeline", "surface"],
)
_cdc_feed_files_total = Counter(
    "viaduck_cdc_feed_files_total",
    "Data files enumerated by the direct-SQL feed",
    ["pipeline"],
)
_cdc_feed_inlined_rows_total = Counter(
    "viaduck_cdc_feed_inlined_rows_total",
    "Rows read from ducklake inline stores by the direct-SQL feed",
    ["pipeline"],
)
# Plan/execute-skew re-plans (a listed file vanished between catalog read and
# parquet GET — merge/expiry race). Nonzero is expected occasionally during
# compaction; a sustained rate means the re-plan loop is fighting the
# compactor and the alert belongs on the flush path, not here.
_cdc_feed_replans_total = Counter(
    "viaduck_cdc_feed_replans_total",
    "Feed reads re-planned after a listed file vanished (plan/execute skew)",
    ["pipeline"],
)
# The absorbing-regime detector (log-consumer-proposal.md §8): distinct
# read-unit clusters per poll cycle. At head-converged this is 1; a growth
# trend means cursor scatter is forming — THE regression alarm for the new
# read loop (its predecessor was the timeboxed-cycles counter).
_read_clusters = Gauge(
    "viaduck_read_clusters",
    "Distinct read-unit clusters dispatched this poll cycle",
    ["pipeline"],
)
_read_pool_inflight = Gauge(
    "viaduck_read_pool_inflight",
    "Unit reads dispatched but not yet applied (barrier occupancy)",
    ["pipeline"],
)
# Source columns viaduck refuses to replicate because pyducklake cannot
# represent their DuckDB type (e.g. VARIANT, which also cannot cross the
# DuckDB→Arrow boundary as of duckdb 1.5.5). Incremented once per excluded
# column per source-schema load; a nonzero value means the destination is
# missing columns the source has — deliberate, but worth a dashboard.
_source_columns_excluded_total = Counter(
    "viaduck_source_columns_excluded_total",
    "Source columns excluded from replication due to unrepresentable types",
    ["pipeline", "column", "type"],
)
_source_snapshot_id = Gauge(
    "viaduck_source_snapshot_id",
    "Current source snapshot ID",
    ["pipeline"],
)

# Explicit buckets: the OCC retry policy in apply._write_with_retry can
# stretch a single write to several minutes under sustained peer-writer
# pressure. Default prometheus_client buckets top out at 10s, which collapses
# the entire retry-storm range into +Inf and makes p95/p99 uninformative.
# The bucket set spans from fast-path writes (~1s) up to the retry-budget
# worst case (~5-7 min).
_WRITE_LATENCY_BUCKETS = (0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0)

_dest_write_seconds = Histogram(
    "viaduck_dest_write_seconds",
    "Time per destination write",
    ["pipeline", "destination"],
    buckets=_WRITE_LATENCY_BUCKETS,
)
_dest_rows_written_total = Counter(
    "viaduck_dest_rows_written_total",
    "Rows written to destination (at-least-once: a range replayed after a commit/cursor-gap failure is counted again)",
    ["pipeline", "destination"],
)
_dest_last_snapshot_id = Gauge(
    "viaduck_dest_last_snapshot_id",
    "Last replicated snapshot per destination",
    ["pipeline", "destination"],
)
_dest_lag_snapshots = Gauge(
    "viaduck_dest_lag_snapshots",
    "Snapshot lag per destination (current - last_replicated)",
    ["pipeline", "destination"],
)
_dest_time_lag_seconds = Gauge(
    "viaduck_dest_time_lag_seconds",
    "Wall-clock age of the destination's last flushed source snapshot "
    "(now - ducklake_snapshot.snapshot_time) — exact time lag, unlike the "
    "snapshot-count lag, which needs a commit-rate assumption to convert",
    ["pipeline", "destination"],
)
_dest_flush_target_bytes = Gauge(
    "viaduck_dest_flush_target_bytes",
    "Adaptive per-destination flush-size target (the bytes flush-trigger "
    "threshold): starts at delivery.flush_max_bytes and AIMD-adapts to "
    "observed flush duration — a target pinned at the floor marks a "
    "commit-contended destination catalog",
    ["pipeline", "destination"],
)

_unrouted_rows_total = Counter(
    "viaduck_unrouted_rows_total",
    "Rows with no matching destination (dropped)",
    ["pipeline"],
)

_pool_open_connections = Gauge(
    "viaduck_pool_open_connections",
    "Currently open destination connections",
    ["pipeline"],
)
_pool_evictions_total = Counter(
    "viaduck_pool_evictions_total",
    "LRU connection pool evictions",
    ["pipeline"],
)
# Force-evictions from the write-retry path, split by why. Each one is a full
# Catalog close+recreate (10-20s re-ATTACH, plus the known ~160MB native
# leak per close — see apply._write_with_retry) so a climbing rate is an
# incident signal, not noise: `connection` = network/RDS death, and
# `instance_fatal` = a DuckDB instance invalidated by an Internal/Fatal
# error (fork bug — always worth eyes).
_pool_force_evictions_total = Counter(
    "viaduck_pool_force_evictions_total",
    "Write-retry force-evictions of a destination's pooled catalog, by reason",
    ["pipeline", "destination", "reason"],
)
_pool_creates_total = Counter(
    "viaduck_pool_creates_total",
    "New destination connections created",
    ["pipeline"],
)

_errors_total = Counter(
    "viaduck_errors_total",
    "Errors by type and destination",
    ["pipeline", "type", "destination"],
)

# CDC-specific metrics
_cdc_batch_rows = Histogram(
    "viaduck_cdc_batch_rows",
    "Number of rows per CDC read from source",
    ["pipeline"],
    buckets=[100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000, 5000000, 10000000],
)
_dest_rows_deleted_total = Counter(
    "viaduck_dest_rows_deleted_total",
    "Rows deleted from destination via CDC (at-least-once: replays re-count)",
    ["pipeline", "destination"],
)
_dest_rows_upserted_total = Counter(
    "viaduck_dest_rows_upserted_total",
    "Rows sent to upsert (insert + update via MERGE) to destination (at-least-once: replays re-count)",
    ["pipeline", "destination"],
)
_dest_upsert_matched_total = Counter(
    "viaduck_dest_upsert_matched_total",
    "Rows that matched existing rows during upsert (updated, not inserted) (at-least-once: replays re-count)",
    ["pipeline", "destination"],
)
_cdc_routing_mutations_total = Counter(
    "viaduck_cdc_routing_mutations_total",
    "Cross-tenant routing value changes detected in updates",
    ["pipeline"],
)
_cdc_tombstones_emitted_total = Counter(
    "viaduck_cdc_tombstones_emitted_total",
    "Deletes surviving Phase 2 from insert+delete pairs (tombstones): "
    "normally no-ops at the destination; the write cost of phantom healing",
    ["pipeline"],
)
_cdc_conflicts_resolved_total = Counter(
    "viaduck_cdc_conflicts_resolved_total",
    "Rowid-level conflicts resolved in Phase 2 (insert+postimage, insert+delete)",
    ["pipeline"],
)
_cdc_orphaned_preimages_total = Counter(
    "viaduck_cdc_orphaned_preimages_total",
    "Update preimages with no matching postimage (converted to deletes)",
    ["pipeline"],
)
_delivery_buffer_rows = Gauge(
    "viaduck_delivery_buffer_rows",
    "Rows currently buffered awaiting flush",
    ["pipeline", "destination"],
)
_delivery_buffer_bytes = Gauge(
    "viaduck_delivery_buffer_bytes",
    "Bytes currently buffered awaiting flush",
    ["pipeline", "destination"],
)
_delivery_buffer_total_bytes = Gauge(
    "viaduck_delivery_buffer_total_bytes",
    "Bytes buffered or in flight to flush workers, across all destinations",
    ["pipeline"],
)
_delivery_flushes_total = Counter(
    "viaduck_delivery_flushes_total",
    "Completed destination flushes by trigger (interval/rows/bytes/memory/sliced/shutdown)",
    ["pipeline", "destination", "trigger"],
)
_delivery_flush_seconds = Histogram(
    "viaduck_delivery_flush_seconds",
    "Wall time of a destination flush (conflict resolution + write + cursor)",
    ["pipeline", "destination"],
    buckets=_WRITE_LATENCY_BUCKETS,
)
_delivery_reads_paused = Gauge(
    "viaduck_delivery_reads_paused",
    "1 while a destination's queue (buffer + in-flight) is at its per-destination cap and CDC reads for it are paused",
    ["pipeline", "destination"],
)
_delivery_circuit_open = Gauge(
    "viaduck_delivery_circuit_open",
    "1 from the Nth consecutive flush failure until a DATA flush succeeds (position-only persists don't prove "
    "health). Submissions resume as a probe after each backoff, so 1 reads 'unproven', not 'paused' — an idle "
    "destination can hold 1 until traffic returns.",
    ["pipeline", "destination"],
)
_delivery_circuit_opens_total = Counter(
    "viaduck_delivery_circuit_opens_total",
    "Times a destination's flush circuit breaker opened (consecutive flush failures reached the threshold)",
    ["pipeline", "destination"],
)
_delivery_flush_deadlines_total = Counter(
    "viaduck_delivery_flush_deadlines_total",
    "Flushes aborted by the overall flush deadline (retry loop exceeded delivery.flush_deadline_seconds)",
    ["pipeline", "destination"],
)
_destination_lifecycle_state = Gauge(
    "viaduck_destination_lifecycle_state",
    "1 for the destination's current lifecycle state (active|paused|draining|retired); see viaduck/lifecycle.py",
    ["pipeline", "destination", "state"],
)
_lifecycle_discarded_rows_total = Counter(
    "viaduck_lifecycle_discarded_rows_total",
    "Buffered rows discarded by a lifecycle pause/retire (durable in the source; re-read from the cursor on resume)",
    ["pipeline", "destination"],
)
_retention_clamp_total = Counter(
    "viaduck_retention_clamp_total",
    "Retention-edge cursor clamps: the destination's cursor fell below the earliest retained source "
    "snapshot and was advanced to the edge. outcome=lost — the range was never read: unrecoverable "
    "data loss, alert on it. outcome=at_risk — the range is buffered/in-flight and is lost only if "
    "its pending flush fails",
    ["pipeline", "destination", "outcome"],
)
_secret_cache_stale_fallback_total = Counter(
    "viaduck_secret_cache_stale_fallback_total",
    "Secret reads served from the stale cache after an API failure (k8s_secrets.read_secret_key_cached). "
    "Sustained increments = flushing on possibly-rotated credentials during an API-server outage — alertable",
    ["pipeline"],
)
_discovery_synced = Gauge(
    "viaduck_discovery_synced",
    "1 after a successful STARTUP discovery poll; 0 = static-only (CP unreachable at startup). Startup-owned: "
    "the drift poller never moves it — read staleness from discovery_last_success_timestamp_seconds instead",
    ["pipeline"],
)
_discovery_config_generation = Gauge(
    "viaduck_discovery_config_generation",
    "config_generation from the last successful discovery poll (opaque change token; compare for equality)",
    ["pipeline"],
)
_discovery_last_success_timestamp_seconds = Gauge(
    "viaduck_discovery_last_success_timestamp_seconds",
    "Unix time of the last successful discovery poll (alert on age)",
    ["pipeline"],
)
_discovery_poll_failures_total = Counter(
    "viaduck_discovery_poll_failures_total",
    "Failed discovery polls (last view kept)",
    ["pipeline"],
)
_discovery_broken_entries_total = Counter(
    "viaduck_discovery_broken_entries_total",
    "Discovery entries skipped, by reason (not_writable|no_bucket|no_metadata_store|no_secret_ref|...)",
    ["pipeline", "reason"],
)
_discovery_destinations = Gauge(
    "viaduck_discovery_destinations",
    "Destinations materialized from discovery at startup (fixed for the process lifetime until C3)",
    ["pipeline"],
)
_discovery_drift_destinations = Gauge(
    "viaduck_discovery_drift_destinations",
    "Live payload vs the STARTUP baseline (kind=added|removed|changed). With discovery.apply_enabled "
    "this gauge is not updated (the reconciler applies changes; pending work is on "
    "viaduck_reconciler_pending); without it, nonzero = restart to apply",
    ["pipeline", "kind"],
)
_discovery_applied_generation = Gauge(
    "viaduck_discovery_applied_generation",
    "config_generation of the last CP view the reconciler FULLY applied (every fired primitive "
    "succeeded; deliberate deferrals excluded). applied == served is the new discovery_synced",
    ["pipeline"],
)
_discovery_applied_total = Counter(
    "viaduck_discovery_applied_total",
    "Reconciler primitives executed (kind=start|stop|restart)",
    ["pipeline", "kind"],
)
_reconciler_pending = Gauge(
    "viaduck_reconciler_pending",
    "Deliberately deferred reconciler work by reason "
    "(debounce|static_suppressed|floor|pending_restart|budget|rate_capped|retired|failure)",
    ["pipeline", "reason"],
)
_discovery_stop_countdown = Gauge(
    "viaduck_discovery_stop_countdown",
    "Clean fetches remaining before a running-but-absent discovered destination deactivates "
    "(absent_stop_fetches minus the current absent streak); removed when the id is mentioned again or stopped",
    ["pipeline", "destination"],
)
_discovery_classified = Gauge(
    "viaduck_discovery_classified",
    "Destinations in the last classified CP view (startable|mentioned_only). Mentioned-only tenants "
    "(fenced, degraded — see discovery_broken_entries_total for why) are never absent; ABSENT is "
    "derived (registry-minus-view), never a payload classification",
    ["pipeline", "classification"],
)
_discovery_view_poisoned = Gauge(
    "viaduck_discovery_view_poisoned",
    "1 when the last classified view contained un-enumerable content (unparseable warehouse or "
    "unnameable team row) — absence evaluation is frozen for the whole view while it is set",
    ["pipeline"],
)
_discovery_drift_transitions_total = Counter(
    "viaduck_discovery_drift_transitions_total",
    "Drift state transitions (kind=added|removed|changed|unwritable) — alertable, unlike gauge flaps",
    ["pipeline", "kind"],
)
_delivery_buffers_dropped_total = Counter(
    "viaduck_delivery_buffers_dropped_total",
    "Buffers discarded after a failed flush (range will be re-read)",
    ["pipeline", "destination"],
)
# Pair-split phantom fix: entries dropped at flush-commit because the
# commit already covered their range (pause/zombie replay leftovers).
# Nonzero is the visible trace of that race having fired.
_delivery_covered_replays_dropped_total = Counter(
    "viaduck_delivery_covered_replays_dropped_total",
    "Buffered replay entries dropped at flush commit (already covered by it)",
    ["pipeline", "destination"],
)

# Partition-spec outcomes from `_ensure_partition_spec` (destination.py).
# Operators need to verify across N destinations after a deploy that
# turns on `partition_by` — without this counter, that verification is
# grep-the-logs. Outcomes:
#   applied             — we ran the ALTER successfully
#   applied_by_peer     — we lost the race; a peer pod's spec is what we wanted
#   refused_populated   — table has data + gate is off (covers true-populated
#                          AND probe-failure-treat-as-populated paths; the
#                          distinguishing signal is the ERROR log line in
#                          _table_has_data); left unpartitioned
#   skipped_matches     — table already partitioned with the spec we'd apply
#   skipped_diverges    — table partitioned with a DIFFERENT spec (operator must reconcile)
#   skipped_no_config   — partition_by empty in config (default — no-op)
_partition_spec_total = Counter(
    "viaduck_partition_spec_total",
    "Outcomes from _ensure_partition_spec by destination + outcome label",
    ["pipeline", "destination", "outcome"],
)

# OCC retry surface. Every failed attempt inside apply._write_with_retry
# bumps `dest_write_retries_total{destination}`; the retry loop set/clears
# `dest_write_retrying{destination}` at 1/0 around the whole loop so poll-
# thread pauses / noisy-neighbor incidents are diagnosable from Grafana
# without grepping WARN logs. The gauge is what makes the "one stuck
# destination pins the shared buffer watermark for minutes" case legible.
_dest_write_retries_total = Counter(
    "viaduck_dest_write_retries_total",
    "Failed OCC write attempts per destination (does not count the successful attempt)",
    ["pipeline", "destination"],
)
_dest_write_retrying = Gauge(
    "viaduck_dest_write_retrying",
    "1 while a destination flush is inside its OCC retry loop, 0 otherwise",
    ["pipeline", "destination"],
)

# Per-value cast fallbacks in schema_projection._cast_column. The whole-batch
# `pc.cast` is all-or-nothing; on ArrowInvalid we fall back per-value and null
# the unparseable values, mirroring millpond's `_coerce_or_null`. This counter
# is the alarm signal: if this ever ticks in prod it means a producer format
# drift is silently nulling data — one row per null.
_projection_cast_null_fallback_total = Counter(
    "viaduck_projection_cast_null_fallback_total",
    "Values nulled by per-value fallback in schema_projection._cast_column",
    ["pipeline", "destination", "column"],
)

# --- Public names (replaced by init() with pipeline-bound instances) ---

polls_total = _polls_total
self_recycles_total = _self_recycles_total
cdc_read_seconds = _cdc_read_seconds
cdc_rows_read_total = _cdc_rows_read_total
cdc_feed_query_seconds = _cdc_feed_query_seconds
cdc_feed_files_total = _cdc_feed_files_total
cdc_feed_inlined_rows_total = _cdc_feed_inlined_rows_total
cdc_feed_replans_total = _cdc_feed_replans_total
read_clusters = _read_clusters
read_pool_inflight = _read_pool_inflight
source_columns_excluded_total = _source_columns_excluded_total
source_snapshot_id = _source_snapshot_id

dest_write_seconds = _dest_write_seconds
dest_rows_written_total = _dest_rows_written_total
dest_last_snapshot_id = _dest_last_snapshot_id
dest_lag_snapshots = _dest_lag_snapshots
dest_time_lag_seconds = _dest_time_lag_seconds
dest_flush_target_bytes = _dest_flush_target_bytes

unrouted_rows_total = _unrouted_rows_total

pool_open_connections = _pool_open_connections
pool_evictions_total = _pool_evictions_total
pool_creates_total = _pool_creates_total

errors_total = _errors_total

cdc_batch_rows = _cdc_batch_rows
dest_rows_deleted_total = _dest_rows_deleted_total
dest_rows_upserted_total = _dest_rows_upserted_total
dest_upsert_matched_total = _dest_upsert_matched_total
cdc_routing_mutations_total = _cdc_routing_mutations_total
cdc_conflicts_resolved_total = _cdc_conflicts_resolved_total
cdc_tombstones_emitted_total = _cdc_tombstones_emitted_total
cdc_orphaned_preimages_total = _cdc_orphaned_preimages_total

delivery_buffer_rows = _delivery_buffer_rows
delivery_buffer_bytes = _delivery_buffer_bytes
delivery_buffer_total_bytes = _delivery_buffer_total_bytes
delivery_flushes_total = _delivery_flushes_total
delivery_flush_seconds = _delivery_flush_seconds
delivery_buffers_dropped_total = _delivery_buffers_dropped_total
delivery_covered_replays_dropped_total = _delivery_covered_replays_dropped_total
delivery_reads_paused = _delivery_reads_paused
delivery_circuit_open = _delivery_circuit_open
delivery_circuit_opens_total = _delivery_circuit_opens_total
delivery_flush_deadlines_total = _delivery_flush_deadlines_total
destination_lifecycle_state = _destination_lifecycle_state
lifecycle_discarded_rows_total = _lifecycle_discarded_rows_total
retention_clamp_total = _retention_clamp_total
secret_cache_stale_fallback_total = _secret_cache_stale_fallback_total
discovery_synced = _discovery_synced
discovery_config_generation = _discovery_config_generation
discovery_last_success_timestamp_seconds = _discovery_last_success_timestamp_seconds
discovery_poll_failures_total = _discovery_poll_failures_total
discovery_broken_entries_total = _discovery_broken_entries_total
discovery_destinations = _discovery_destinations
discovery_drift_destinations = _discovery_drift_destinations
discovery_drift_transitions_total = _discovery_drift_transitions_total
discovery_classified = _discovery_classified
discovery_view_poisoned = _discovery_view_poisoned
discovery_applied_generation = _discovery_applied_generation
discovery_applied_total = _discovery_applied_total
reconciler_pending = _reconciler_pending
discovery_stop_countdown = _discovery_stop_countdown

partition_spec_total = _partition_spec_total
projection_cast_null_fallback_total = _projection_cast_null_fallback_total
dest_write_retries_total = _dest_write_retries_total
dest_write_retrying = _dest_write_retrying
pool_force_evictions_total = _pool_force_evictions_total


def set_destination_lifecycle(dest_id: str, state: str, all_states) -> None:
    """One-hot lifecycle gauge (mode-gauge pattern): exactly one state label
    is 1 per destination, the rest 0 — so fleet queries can't read a stale
    1 from the previous state."""
    for s in all_states:
        destination_lifecycle_state.labels(destination=dest_id, state=s).set(1 if s == state else 0)


def init(pipeline: str):
    """Bind all metrics to a pipeline label. Must be called once at startup."""
    global polls_total, cdc_read_seconds, cdc_rows_read_total, source_snapshot_id
    global cdc_feed_query_seconds, cdc_feed_files_total, cdc_feed_inlined_rows_total, cdc_feed_replans_total
    global read_clusters, read_pool_inflight
    global self_recycles_total
    global source_columns_excluded_total
    global dest_write_seconds, dest_rows_written_total, dest_last_snapshot_id, dest_lag_snapshots
    global dest_time_lag_seconds, dest_flush_target_bytes
    global unrouted_rows_total
    global pool_open_connections, pool_evictions_total, pool_creates_total
    global errors_total
    global cdc_batch_rows
    global dest_rows_deleted_total, dest_rows_upserted_total, dest_upsert_matched_total
    global cdc_routing_mutations_total, cdc_conflicts_resolved_total, cdc_orphaned_preimages_total
    global cdc_tombstones_emitted_total
    global delivery_buffer_rows, delivery_buffer_bytes, delivery_buffer_total_bytes
    global delivery_flushes_total, delivery_flush_seconds, delivery_buffers_dropped_total
    global delivery_covered_replays_dropped_total
    global delivery_reads_paused, destination_lifecycle_state, lifecycle_discarded_rows_total
    global delivery_circuit_open, delivery_circuit_opens_total, delivery_flush_deadlines_total
    global retention_clamp_total, secret_cache_stale_fallback_total
    global discovery_synced, discovery_config_generation, discovery_last_success_timestamp_seconds
    global discovery_poll_failures_total, discovery_broken_entries_total
    global discovery_destinations, discovery_drift_destinations, discovery_drift_transitions_total
    global discovery_classified, discovery_view_poisoned
    global discovery_applied_generation, discovery_applied_total, reconciler_pending, discovery_stop_countdown
    global partition_spec_total, projection_cast_null_fallback_total
    global dest_write_retries_total, dest_write_retrying
    global pool_force_evictions_total

    # Metrics with additional labels — wrap so .labels() auto-injects pipeline
    dest_write_seconds = _AutoPipelineLabels(_dest_write_seconds, pipeline)
    delivery_buffer_rows = _AutoPipelineLabels(_delivery_buffer_rows, pipeline)
    delivery_buffer_bytes = _AutoPipelineLabels(_delivery_buffer_bytes, pipeline)
    delivery_flushes_total = _AutoPipelineLabels(_delivery_flushes_total, pipeline)
    delivery_flush_seconds = _AutoPipelineLabels(_delivery_flush_seconds, pipeline)
    delivery_buffers_dropped_total = _AutoPipelineLabels(_delivery_buffers_dropped_total, pipeline)
    delivery_covered_replays_dropped_total = _AutoPipelineLabels(_delivery_covered_replays_dropped_total, pipeline)
    delivery_reads_paused = _AutoPipelineLabels(_delivery_reads_paused, pipeline)
    delivery_circuit_open = _AutoPipelineLabels(_delivery_circuit_open, pipeline)
    delivery_circuit_opens_total = _AutoPipelineLabels(_delivery_circuit_opens_total, pipeline)
    delivery_flush_deadlines_total = _AutoPipelineLabels(_delivery_flush_deadlines_total, pipeline)
    destination_lifecycle_state = _AutoPipelineLabels(_destination_lifecycle_state, pipeline)
    lifecycle_discarded_rows_total = _AutoPipelineLabels(_lifecycle_discarded_rows_total, pipeline)
    retention_clamp_total = _AutoPipelineLabels(_retention_clamp_total, pipeline)
    discovery_broken_entries_total = _AutoPipelineLabels(_discovery_broken_entries_total, pipeline)
    discovery_drift_destinations = _AutoPipelineLabels(_discovery_drift_destinations, pipeline)
    discovery_drift_transitions_total = _AutoPipelineLabels(_discovery_drift_transitions_total, pipeline)
    discovery_classified = _AutoPipelineLabels(_discovery_classified, pipeline)
    discovery_view_poisoned = _discovery_view_poisoned.labels(pipeline=pipeline)
    discovery_applied_generation = _discovery_applied_generation.labels(pipeline=pipeline)
    discovery_applied_total = _AutoPipelineLabels(_discovery_applied_total, pipeline)
    reconciler_pending = _AutoPipelineLabels(_reconciler_pending, pipeline)
    discovery_stop_countdown = _AutoPipelineLabels(_discovery_stop_countdown, pipeline)
    secret_cache_stale_fallback_total = _secret_cache_stale_fallback_total.labels(pipeline=pipeline)
    discovery_synced = _discovery_synced.labels(pipeline=pipeline)
    discovery_config_generation = _discovery_config_generation.labels(pipeline=pipeline)
    discovery_last_success_timestamp_seconds = _discovery_last_success_timestamp_seconds.labels(pipeline=pipeline)
    discovery_poll_failures_total = _discovery_poll_failures_total.labels(pipeline=pipeline)
    discovery_destinations = _discovery_destinations.labels(pipeline=pipeline)
    dest_rows_written_total = _AutoPipelineLabels(_dest_rows_written_total, pipeline)
    dest_last_snapshot_id = _AutoPipelineLabels(_dest_last_snapshot_id, pipeline)
    dest_lag_snapshots = _AutoPipelineLabels(_dest_lag_snapshots, pipeline)
    dest_time_lag_seconds = _AutoPipelineLabels(_dest_time_lag_seconds, pipeline)
    dest_flush_target_bytes = _AutoPipelineLabels(_dest_flush_target_bytes, pipeline)
    errors_total = _AutoPipelineLabels(_errors_total, pipeline)
    dest_rows_deleted_total = _AutoPipelineLabels(_dest_rows_deleted_total, pipeline)
    dest_rows_upserted_total = _AutoPipelineLabels(_dest_rows_upserted_total, pipeline)
    partition_spec_total = _AutoPipelineLabels(_partition_spec_total, pipeline)
    projection_cast_null_fallback_total = _AutoPipelineLabels(_projection_cast_null_fallback_total, pipeline)
    dest_upsert_matched_total = _AutoPipelineLabels(_dest_upsert_matched_total, pipeline)
    dest_write_retries_total = _AutoPipelineLabels(_dest_write_retries_total, pipeline)
    dest_write_retrying = _AutoPipelineLabels(_dest_write_retrying, pipeline)
    pool_force_evictions_total = _AutoPipelineLabels(_pool_force_evictions_total, pipeline)

    # Metrics with no other labels — pre-label to get direct .inc()/.set()/.observe()
    polls_total = _polls_total.labels(pipeline=pipeline)
    self_recycles_total = _self_recycles_total.labels(pipeline=pipeline)
    cdc_read_seconds = _cdc_read_seconds.labels(pipeline=pipeline)
    cdc_rows_read_total = _cdc_rows_read_total.labels(pipeline=pipeline)
    cdc_feed_query_seconds = _AutoPipelineLabels(_cdc_feed_query_seconds, pipeline)
    cdc_feed_files_total = _cdc_feed_files_total.labels(pipeline=pipeline)
    cdc_feed_inlined_rows_total = _cdc_feed_inlined_rows_total.labels(pipeline=pipeline)
    cdc_feed_replans_total = _cdc_feed_replans_total.labels(pipeline=pipeline)
    read_clusters = _read_clusters.labels(pipeline=pipeline)
    read_pool_inflight = _read_pool_inflight.labels(pipeline=pipeline)
    source_columns_excluded_total = _AutoPipelineLabels(_source_columns_excluded_total, pipeline)
    source_snapshot_id = _source_snapshot_id.labels(pipeline=pipeline)
    delivery_buffer_total_bytes = _delivery_buffer_total_bytes.labels(pipeline=pipeline)
    unrouted_rows_total = _unrouted_rows_total.labels(pipeline=pipeline)
    pool_open_connections = _pool_open_connections.labels(pipeline=pipeline)
    pool_evictions_total = _pool_evictions_total.labels(pipeline=pipeline)
    pool_creates_total = _pool_creates_total.labels(pipeline=pipeline)
    cdc_batch_rows = _cdc_batch_rows.labels(pipeline=pipeline)
    cdc_routing_mutations_total = _cdc_routing_mutations_total.labels(pipeline=pipeline)
    cdc_conflicts_resolved_total = _cdc_conflicts_resolved_total.labels(pipeline=pipeline)
    cdc_tombstones_emitted_total = _cdc_tombstones_emitted_total.labels(pipeline=pipeline)
    cdc_orphaned_preimages_total = _cdc_orphaned_preimages_total.labels(pipeline=pipeline)


def remove_destination_series(dest_id: str) -> None:
    """Best-effort removal of a deactivated destination's per-destination
    series, so a stopped tenant doesn't read as lagging forever. Callers
    (the reconciler) defer this until the id has nothing in flight — an
    in-flight flush calling .labels() after a remove re-creates the
    series frozen. Absent children are fine (KeyError swallowed):
    removal is re-applied idempotently per reconcile cycle."""
    per_dest = (
        dest_lag_snapshots,
        dest_time_lag_seconds,
        dest_last_snapshot_id,
        dest_flush_target_bytes,
        delivery_buffer_rows,
        delivery_buffer_bytes,
        delivery_reads_paused,
        delivery_circuit_open,
        delivery_circuit_opens_total,
        delivery_flush_deadlines_total,
        discovery_stop_countdown,
    )
    for m in per_dest:
        try:
            m.remove(dest_id)
        except KeyError:
            pass
    # Function-level import: lifecycle imports metrics at module top, so a
    # module-level import here would be circular.
    from viaduck import lifecycle as _lc

    for state in _lc.VALID_STATES:
        try:
            destination_lifecycle_state.remove(dest_id, state)
        except KeyError:
            pass
