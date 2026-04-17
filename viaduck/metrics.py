from prometheus_client import Counter, Gauge, Histogram


class _AutoPipelineLabels:
    """Wrapper that auto-injects the pipeline label into .labels() calls."""

    def __init__(self, metric, pipeline: str):
        self._metric = metric
        self._pipeline = pipeline

    def labels(self, **kwargs):
        return self._metric.labels(pipeline=self._pipeline, **kwargs)


# --- Raw metric definitions (with pipeline as first label) ---

_polls_total = Counter(
    "viaduck_polls_total",
    "Poll cycles executed",
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
_source_snapshot_id = Gauge(
    "viaduck_source_snapshot_id",
    "Current source snapshot ID",
    ["pipeline"],
)

_dest_write_seconds = Histogram(
    "viaduck_dest_write_seconds",
    "Time per destination write",
    ["pipeline", "destination"],
)
_dest_rows_written_total = Counter(
    "viaduck_dest_rows_written_total",
    "Rows written to destination",
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

# --- Public names (replaced by init() with pipeline-bound instances) ---

polls_total = _polls_total
cdc_read_seconds = _cdc_read_seconds
cdc_rows_read_total = _cdc_rows_read_total
source_snapshot_id = _source_snapshot_id

dest_write_seconds = _dest_write_seconds
dest_rows_written_total = _dest_rows_written_total
dest_last_snapshot_id = _dest_last_snapshot_id
dest_lag_snapshots = _dest_lag_snapshots

unrouted_rows_total = _unrouted_rows_total

pool_open_connections = _pool_open_connections
pool_evictions_total = _pool_evictions_total
pool_creates_total = _pool_creates_total

errors_total = _errors_total


def init(pipeline: str):
    """Bind all metrics to a pipeline label. Must be called once at startup."""
    global polls_total, cdc_read_seconds, cdc_rows_read_total, source_snapshot_id
    global dest_write_seconds, dest_rows_written_total, dest_last_snapshot_id, dest_lag_snapshots
    global unrouted_rows_total
    global pool_open_connections, pool_evictions_total, pool_creates_total
    global errors_total

    # Metrics with additional labels — wrap so .labels() auto-injects pipeline
    dest_write_seconds = _AutoPipelineLabels(_dest_write_seconds, pipeline)
    dest_rows_written_total = _AutoPipelineLabels(_dest_rows_written_total, pipeline)
    dest_last_snapshot_id = _AutoPipelineLabels(_dest_last_snapshot_id, pipeline)
    dest_lag_snapshots = _AutoPipelineLabels(_dest_lag_snapshots, pipeline)
    errors_total = _AutoPipelineLabels(_errors_total, pipeline)

    # Metrics with no other labels — pre-label to get direct .inc()/.set()/.observe()
    polls_total = _polls_total.labels(pipeline=pipeline)
    cdc_read_seconds = _cdc_read_seconds.labels(pipeline=pipeline)
    cdc_rows_read_total = _cdc_rows_read_total.labels(pipeline=pipeline)
    source_snapshot_id = _source_snapshot_id.labels(pipeline=pipeline)
    unrouted_rows_total = _unrouted_rows_total.labels(pipeline=pipeline)
    pool_open_connections = _pool_open_connections.labels(pipeline=pipeline)
    pool_evictions_total = _pool_evictions_total.labels(pipeline=pipeline)
    pool_creates_total = _pool_creates_total.labels(pipeline=pipeline)
