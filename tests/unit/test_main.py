"""Tests for main loop logic."""

from __future__ import annotations

from datetime import UTC
from unittest.mock import MagicMock, call, patch

import pyarrow as pa
import pytest

from viaduck import metrics
from viaduck.apply import (
    _apply_changes,
    _build_delete_filter,
    _resolve_conflicts,
    _write_with_retry,
    append_only,
)
from viaduck.main import (
    _clamp_expired_cursors,
    _derive_dest_status,
    _fmt_duration,
    _group_by_cursor,
    _initial_snapshot_id,
    _poll_cycle,
    _resolve_preimages,
    _scan_progress_suffix,
    _seed_new_destinations,
    _start_progress_heartbeat,
)
from viaduck.router import RoutingError


def setup_module():
    metrics.init("test")


# ---------------------------------------------------------------------------
# _group_by_cursor
# ---------------------------------------------------------------------------


def test_group_by_cursor_all_same():
    cursors = {"a": 10, "b": 10, "c": 10}
    groups = _group_by_cursor(cursors, ["a", "b", "c"])
    assert groups == {10: ["a", "b", "c"]}


def test_group_by_cursor_mixed():
    cursors = {"a": 10, "b": 5, "c": 10}
    groups = _group_by_cursor(cursors, ["a", "b", "c"])
    assert groups == {10: ["a", "c"], 5: ["b"]}


def test_group_by_cursor_missing_defaults_to_zero():
    cursors = {"a": 10}
    groups = _group_by_cursor(cursors, ["a", "b"])
    assert groups == {10: ["a"], 0: ["b"]}


def test_group_by_cursor_empty():
    groups = _group_by_cursor({}, [])
    assert groups == {}


# ---------------------------------------------------------------------------
# _write_with_retry
# ---------------------------------------------------------------------------


def test_write_with_retry_success_first_attempt():
    pool = MagicMock()
    mock_catalog = MagicMock()
    mock_table = MagicMock()
    pool.get.return_value = (mock_catalog, mock_table)
    called = {}

    def op(catalog, table):
        called["catalog"] = catalog
        called["table"] = table

    _write_with_retry(pool, "dest-1", op)

    assert called["catalog"] is mock_catalog
    assert called["table"] is mock_table


def test_write_with_retry_retries_on_failure():
    pool = MagicMock()
    mock_catalog = MagicMock()
    mock_table = MagicMock()
    pool.get.return_value = (mock_catalog, mock_table)
    attempts = {"count": 0}

    def op(catalog, table):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise Exception("fail")

    with patch("viaduck.apply.time.sleep"):
        _write_with_retry(pool, "dest-1", op)

    assert attempts["count"] == 2
    # A generic (non-connection) failure retries on the SAME connection and
    # must NOT evict/reconnect: closing mid-write orphaned the fork httpfs
    # write-retry buffer (~one flush, 130-160MB native) — the writer OOM leak.
    # Only a positively-identified connection-death signal may reconnect.
    pool.evict.assert_not_called()


def test_write_with_retry_evicts_only_on_connection_error():
    """A genuine connection-death signal reconnects; a plain write/IO error
    does not (evicting on it orphaned the httpfs write-retry buffer = OOM leak)."""
    pool = MagicMock()
    pool.get.return_value = (MagicMock(), MagicMock())
    attempts = {"count": 0}

    def op(catalog, table):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise Exception("server closed the connection unexpectedly")

    with patch("viaduck.apply.time.sleep"):
        _write_with_retry(pool, "dest-1", op)

    assert attempts["count"] == 2
    pool.evict.assert_called_once_with("dest-1")


def test_write_with_retry_exhausted():
    pool = MagicMock()
    pool.get.return_value = (MagicMock(), MagicMock())

    def op(catalog, table):
        raise Exception("persistent failure")

    with patch("viaduck.apply.time.sleep"):
        with pytest.raises(Exception, match="persistent failure"):
            _write_with_retry(pool, "dest-1", op)


def test_write_with_retry_survives_many_transient_failures():
    """team-50689 OCC scenario: peer writer commits at ~12s mean, viaduck flush
    window 2-5s. Half the attempts race a peer commit and fail. Verify the
    retry budget is deep enough that a run of transient failures resolves
    before exhausting attempts."""
    from viaduck.apply import _WRITE_MAX_RETRIES

    assert _WRITE_MAX_RETRIES >= 10, (
        "OCC-heavy destinations (concurrent-writer catalogs) need enough "
        "retry budget to catch a P99 gap; 3 attempts is deterministic-fail "
        "on team-50689-shape workloads"
    )

    pool = MagicMock()
    pool.get.return_value = (MagicMock(), MagicMock())
    attempts = {"count": 0}
    fail_first_n = _WRITE_MAX_RETRIES - 2  # succeed on the second-to-last attempt

    def op(catalog, table):
        attempts["count"] += 1
        if attempts["count"] <= fail_first_n:
            raise Exception(f"transient OCC failure #{attempts['count']}")

    with patch("viaduck.apply.time.sleep"):
        _write_with_retry(pool, "dest-1", op)

    assert attempts["count"] == fail_first_n + 1


def test_write_with_retry_backoff_caps_and_jitters():
    """Verify (a) delay is capped at _WRITE_MAX_DELAY_S so we don't grow
    unbounded across 15 attempts, and (b) jitter perturbs the deterministic
    exp schedule so N concurrent workers don't herd on the same retry tick."""
    from viaduck.apply import _WRITE_BASE_DELAY_S, _WRITE_MAX_DELAY_S, _WRITE_MAX_RETRIES

    pool = MagicMock()
    pool.get.return_value = (MagicMock(), MagicMock())

    def op(catalog, table):
        raise Exception("perma-fail")

    delays: list[float] = []
    with patch("viaduck.apply.time.sleep", side_effect=lambda d: delays.append(d)):
        with pytest.raises(Exception, match="perma-fail"):
            _write_with_retry(pool, "dest-1", op)

    from viaduck.apply import _WRITE_JITTER_FRACTION

    # Every delay is bounded by max * (1 + jitter_fraction), plus a small
    # slack for float rounding.
    upper = _WRITE_MAX_DELAY_S * (1.0 + _WRITE_JITTER_FRACTION) + 0.01
    for d in delays:
        assert 0 < d <= upper, f"delay {d} exceeds cap {upper}"

    # We should have MAX_RETRIES - 1 sleeps (no sleep after the final failure).
    assert len(delays) == _WRITE_MAX_RETRIES - 1

    # Late attempts should hit the cap: the deterministic base at attempt
    # log2(MAX_DELAY / BASE) is where it plateaus. Verify at least one late
    # delay is within the jitter band around _WRITE_MAX_DELAY_S.
    lower_band = _WRITE_MAX_DELAY_S * (1.0 - _WRITE_JITTER_FRACTION)
    upper_band = _WRITE_MAX_DELAY_S * (1.0 + _WRITE_JITTER_FRACTION)
    cap_hit = [d for d in delays[-3:] if lower_band <= d <= upper_band]
    assert cap_hit, f"expected late delays in [{lower_band}, {upper_band}]s, got tail {delays[-3:]}"

    # Jitter: not all delays should be equal to the deterministic exp schedule
    # (would indicate jitter is disabled).
    deterministic = [
        _WRITE_BASE_DELAY_S * min(2**i, _WRITE_MAX_DELAY_S / _WRITE_BASE_DELAY_S) for i in range(len(delays))
    ]
    diffs = [abs(delays[i] - deterministic[i]) for i in range(len(delays))]
    assert any(d > 0.01 for d in diffs), "jitter appears disabled — all delays are the exp base"


def test_write_with_retry_fails_fast_on_schema_projection_error():
    """A SchemaProjectionError raised out of pool.get() (via _create →
    _maybe_build_projection → schema_projection.build) is permanent: the
    source/target schemas don't change between attempts. Retrying wastes
    exponential backoff and logs misleading "Write to X failed" for what is
    a build-time drift error. Must fail fast, not retry."""
    from viaduck.schema_projection import SchemaProjectionError

    pool = MagicMock()
    pool.get.side_effect = SchemaProjectionError("unknown source column 'stowaway'")

    attempts = {"count": 0}

    def op(catalog, table):
        attempts["count"] += 1

    with patch("viaduck.apply.time.sleep") as mock_sleep:
        with pytest.raises(SchemaProjectionError, match="stowaway"):
            _write_with_retry(pool, "dest-1", op)

    assert attempts["count"] == 0, "operation should not even be called on projection build error"
    assert pool.get.call_count == 1, "must not retry on SchemaProjectionError"
    mock_sleep.assert_not_called()
    pool.evict.assert_not_called()


def test_write_with_retry_logs_exception_message():
    """Retry should log the actual exception message. Early attempts (1..3)
    are DEBUG (routine OCC dance under peer-writer pressure); WARN kicks in
    at attempt 4+ so the log channel doesn't drown under sustained retries.
    """
    import logging

    pool = MagicMock()
    pool.get.return_value = (MagicMock(), MagicMock())
    attempts = {"count": 0}

    def op(catalog, table):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise ConnectionError("connection refused")

    with patch("viaduck.apply.time.sleep"), patch("viaduck.apply.log") as mock_log:
        _write_with_retry(pool, "dest-1", op)

    log_call = mock_log.log.call_args
    # attempt 1 → DEBUG
    assert log_call.args[0] == logging.DEBUG, f"attempt 1 should be DEBUG, got {log_call.args[0]}"
    assert "connection refused" in str(log_call)


def test_write_with_retry_log_severity_ladder():
    """Attempts 1..3 log DEBUG; attempt 4 and beyond log WARN. Ensures the
    log signal-to-noise stays useful under sustained OCC retry pressure."""
    import logging

    from viaduck.apply import _WRITE_WARN_ATTEMPT_THRESHOLD

    pool = MagicMock()
    pool.get.return_value = (MagicMock(), MagicMock())

    # Fail exactly `_WRITE_WARN_ATTEMPT_THRESHOLD` times so we exercise both
    # DEBUG (attempts < threshold) and WARN (attempt == threshold).
    attempts = {"count": 0}
    fail_first_n = _WRITE_WARN_ATTEMPT_THRESHOLD

    def op(catalog, table):
        attempts["count"] += 1
        if attempts["count"] <= fail_first_n:
            raise Exception(f"fail #{attempts['count']}")

    with patch("viaduck.apply.time.sleep"), patch("viaduck.apply.log") as mock_log:
        _write_with_retry(pool, "dest-1", op)

    # First (threshold-1) log calls are DEBUG; the threshold-th one is WARN.
    log_calls = mock_log.log.call_args_list
    assert len(log_calls) == fail_first_n, f"expected {fail_first_n} log calls, got {len(log_calls)}"
    for i, log_call in enumerate(log_calls):
        attempt_num = i + 1
        expected = logging.WARNING if attempt_num >= _WRITE_WARN_ATTEMPT_THRESHOLD else logging.DEBUG
        assert log_call.args[0] == expected, f"attempt {attempt_num} logged at {log_call.args[0]}, expected {expected}"


def test_write_with_retry_occ_conflict_does_not_evict():
    """An OCC commit conflict leaves the connection healthy — DuckLake takes
    its snapshot at BeginTransaction, not connect, so retrying on the same
    pooled connection is correct (it's what DuckLake's own internal retry
    did). Evicting per conflict added 10-20s of reconnect per attempt AND
    leaked ~160MB of native memory per pyducklake Catalog create — 274
    reconnects OOM-killed the prod pod at 48Gi six times in one night."""
    pool = MagicMock()
    pool.get.return_value = (MagicMock(), MagicMock())
    attempts = {"count": 0}

    def op(catalog, table):
        attempts["count"] += 1
        if attempts["count"] <= 2:
            raise RuntimeError("TransactionContext Error: Failed to commit: Failed to commit DuckLake transaction.")

    with patch("viaduck.apply._backoff_sleep", return_value=False):
        _write_with_retry(pool, "dest-1", op)

    assert attempts["count"] == 3
    pool.evict.assert_not_called()
    # The retry still re-leases from the pool each attempt (pin/release
    # discipline unchanged) — it just doesn't destroy the entry in between.
    assert pool.get.call_count == 3


def test_write_with_retry_non_occ_error_still_evicts():
    """Connection-implicating errors (network, RDS restart) must still
    evict + reconnect — only OCC conflicts skip the eviction."""
    pool = MagicMock()
    pool.get.return_value = (MagicMock(), MagicMock())
    attempts = {"count": 0}

    def op(catalog, table):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise ConnectionError("connection reset by peer")

    with patch("viaduck.apply._backoff_sleep", return_value=False):
        _write_with_retry(pool, "dest-1", op)

    pool.evict.assert_called_once_with("dest-1")


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ("TransactionContext Error: Failed to commit: Failed to commit DuckLake transaction.", True),
        ('Transaction conflict - attempting to insert into table with index "2354"', True),
        ("connection reset by peer", False),
        ("HTTP Error: HTTP GET error reading 'https://...' (HTTP 404 Not Found)", False),
    ],
)
def test_is_occ_conflict_classification(message, expected):
    from viaduck.apply import _is_occ_conflict

    assert _is_occ_conflict(RuntimeError(message)) is expected


def test_write_with_retry_fails_fast_on_routing_error():
    """RoutingError raised from _build_delete_filter inside the operation
    lambda is permanent (missing key column, config bug) — do NOT burn the
    full retry budget on it."""
    from viaduck.router import RoutingError

    pool = MagicMock()
    pool.get.return_value = (MagicMock(), MagicMock())
    attempts = {"count": 0}

    def op(catalog, table):
        attempts["count"] += 1
        raise RoutingError("key column 'team_id' missing from batch")

    with patch("viaduck.apply.time.sleep") as mock_sleep:
        with pytest.raises(RoutingError, match="team_id"):
            _write_with_retry(pool, "dest-1", op)

    assert attempts["count"] == 1, "must fail fast on RoutingError, not retry"
    mock_sleep.assert_not_called()


def test_write_with_retry_retries_counter_increments():
    """Every failed attempt bumps viaduck_dest_write_retries_total{destination}.
    The successful attempt does NOT — retries is a failure counter, not an
    attempt counter."""
    metrics.init("test_retries_counter")

    pool = MagicMock()
    pool.get.return_value = (MagicMock(), MagicMock())
    attempts = {"count": 0}
    fail_first_n = 3

    def op(catalog, table):
        attempts["count"] += 1
        if attempts["count"] <= fail_first_n:
            raise Exception(f"fail #{attempts['count']}")

    counter = metrics._dest_write_retries_total.labels(pipeline="test_retries_counter", destination="dest-r")
    before = counter._value.get()

    with patch("viaduck.apply.time.sleep"):
        _write_with_retry(pool, "dest-r", op)

    after = counter._value.get()
    assert after - before == fail_first_n, f"expected {fail_first_n} retry bumps, got {after - before}"


def test_write_with_retry_gauge_set_and_cleared():
    """viaduck_dest_write_retrying is 1 during the retry loop and 0 after
    (success OR failure). Set/clear must happen once regardless of the
    number of attempts."""
    metrics.init("test_gauge_scope")

    pool = MagicMock()
    pool.get.return_value = (MagicMock(), MagicMock())
    observed_during: list[float] = []

    def op(catalog, table):
        observed_during.append(
            metrics._dest_write_retrying.labels(pipeline="test_gauge_scope", destination="dest-g")._value.get()
        )

    with patch("viaduck.apply.time.sleep"):
        _write_with_retry(pool, "dest-g", op)

    # During op(), gauge is 1.
    assert observed_during == [1.0], f"expected gauge=1 during op, got {observed_during}"
    # After the retry loop, gauge is 0.
    after = metrics._dest_write_retrying.labels(pipeline="test_gauge_scope", destination="dest-g")._value.get()
    assert after == 0.0, f"gauge should be cleared to 0 after retry loop, got {after}"


def test_write_with_retry_gauge_cleared_on_exhaustion():
    """Same as above but for the exhaustion path — the finally: must clear
    the gauge regardless of how the loop exits."""
    metrics.init("test_gauge_exhaust")

    pool = MagicMock()
    pool.get.return_value = (MagicMock(), MagicMock())

    def op(catalog, table):
        raise Exception("perma-fail")

    with patch("viaduck.apply.time.sleep"):
        with pytest.raises(Exception, match="perma-fail"):
            _write_with_retry(pool, "dest-x", op)

    after = metrics._dest_write_retrying.labels(pipeline="test_gauge_exhaust", destination="dest-x")._value.get()
    assert after == 0.0, "gauge must clear even when retries exhaust"


def test_write_with_retry_stop_event_aborts_retry():
    """SIGTERM path: when the delivery layer sets its stop_event, an
    in-flight retry loop wakes early from the backoff, raises the current
    exception, and never sleeps the full delay. Without this the worker
    ignores SIGTERM and gets SIGKILLed once k8s' termGracePeriodSeconds
    elapses (60s vs the ~5-min retry budget)."""
    import threading

    pool = MagicMock()
    pool.get.return_value = (MagicMock(), MagicMock())

    stop_event = threading.Event()
    attempts = {"count": 0}

    def op(catalog, table):
        attempts["count"] += 1
        # Set the stop event AFTER the first failure so the retry loop
        # observes it during the backoff wait.
        if attempts["count"] == 1:
            stop_event.set()
            raise Exception("first attempt fails")

    # NOTE: no patch on time.sleep — the test wants Event.wait(delay) to
    # actually be called, but with the event set it returns instantly
    # regardless of the delay.
    with pytest.raises(Exception, match="first attempt fails"):
        _write_with_retry(pool, "dest-s", op, stop_event=stop_event)

    assert attempts["count"] == 1, "retry loop should give up after stop_event set, not retry"


def test_write_with_retry_stop_event_none_uses_time_sleep():
    """Without a stop_event, the retry loop uses time.sleep — verify the
    fallback path so an aborted-refactor doesn't silently disable backoff
    for the no-event path."""
    pool = MagicMock()
    pool.get.return_value = (MagicMock(), MagicMock())

    def op(catalog, table):
        raise Exception("perma-fail")

    with patch("viaduck.apply.time.sleep") as mock_sleep:
        with pytest.raises(Exception, match="perma-fail"):
            _write_with_retry(pool, "dest-n", op)

    # 14 backoffs (MAX_RETRIES - 1), all via time.sleep.
    assert mock_sleep.call_count == 14


def test_apply_full_cdc_projection_resolved_per_attempt():
    """After a failed attempt, the retry rebuilds the destination via
    pool.get() → _create → _maybe_build_projection. If projection is captured
    outside the retry loop, a schema drift between attempts is written with
    the STALE plan → silent off-by-one slot corruption (the exact bug class
    schema_projection was written to prevent). Verify projection_for is called
    once per attempt.
    """
    import pyarrow as pa

    from viaduck.apply import apply_full_cdc

    pool = MagicMock()
    pool.get.return_value = (MagicMock(), MagicMock())
    # Track how many times projection_for is queried across attempts.
    pool.projection_for.return_value = None

    # A CDC-shaped batch that survives _resolve_conflicts (one insert row).
    batch = pa.table(
        {
            "change_type": ["insert"],
            "rowid": [pa.scalar(1, type=pa.uint64())],
            "_snapshot_id": [pa.scalar(1, type=pa.uint64())],
            "id": ["a"],
            "val": ["x"],
        }
    )

    attempts = {"count": 0}

    def _apply_side_effect(*args, **kwargs):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise RuntimeError("first attempt fails")
        return {"deleted": 0, "upserted": 1, "upsert_matched": 0}

    with patch("viaduck.apply._apply_changes", side_effect=_apply_side_effect), patch("viaduck.apply.time.sleep"):
        apply_full_cdc(pool, "dest-1", batch, ["id"])

    # Each attempt must independently query projection_for; capture-outside
    # would show exactly 1.
    assert pool.projection_for.call_count == attempts["count"] == 2, (
        f"projection_for should fire once per attempt (got {pool.projection_for.call_count} for "
        f"{attempts['count']} attempts) — closure capture bug is back"
    )


def test_append_only_fallback_metric_not_double_counted_on_retry():
    """The null-fallback metric is the drift-alarm signal for operators. If
    projection.apply runs inside the retry lambda (H2), a transient write
    failure would cause the callback to fire once per attempt for the SAME
    bad rows — reporting 2× or 3× the real drift. Verify the counter
    increments exactly ONCE per completed flush regardless of retry count.
    """
    import pyarrow as pa

    from viaduck import metrics
    from viaduck.apply import append_only

    metrics.init("test_fallback_no_double_count")

    # Real projection with a real fallback trigger (varchar → tz-UTC with
    # one bad row).
    from viaduck.schema_projection import build

    src = pa.schema([pa.field("t", pa.string())])
    tgt = pa.schema([pa.field("t", pa.timestamp("us", tz="UTC"), nullable=True)])
    plan = build(src, tgt)

    pool = MagicMock()
    pool.projection_for.return_value = plan
    mock_table = MagicMock()
    pool.get.return_value = (MagicMock(), mock_table)

    batch = pa.table({"t": ["not-a-date", "2026-06-30 12:00:00"]}, schema=src)

    # First attempt fails, second succeeds — projection runs twice, but the
    # metric should only tick once.
    attempts = {"count": 0}

    def _append_side_effect(_b):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise RuntimeError("first attempt fails")

    mock_table.append.side_effect = _append_side_effect

    counter = metrics._projection_cast_null_fallback_total.labels(
        pipeline="test_fallback_no_double_count", destination="dest-1", column="t"
    )
    before = counter._value.get()

    with patch("viaduck.apply.time.sleep"):
        append_only(pool, "dest-1", batch)

    after = counter._value.get()

    assert attempts["count"] == 2, "sanity: retry actually happened"
    assert after - before == 1, f"fallback metric double-counted across retries: {after - before} (want 1)"


def test_apply_changes_delete_path_skips_projection():
    """Optimization: `_build_delete_filter` only consumes key_columns, and
    B2 guarantees key columns pass through the projection unchanged. So the
    delete path narrows to `key_columns` and does NOT call projection.apply()
    — projection.apply() is only invoked on the upsert path. Regression
    guard: reintroducing projection on delete_rows would trigger cast
    fallback on payload columns for values we discard.
    """
    import pyarrow as pa

    from viaduck.apply import _apply_changes

    projection = MagicMock()
    projection.apply.side_effect = lambda batch, on_null_fallback=None: batch

    batch = pa.table(
        {
            "change_type": ["delete"],
            "rowid": pa.array([1], type=pa.uint64()),
            "snapshot_id": pa.array([1], type=pa.uint64()),
            "id": ["a"],
            "val": ["v-a"],
        }
    )

    catalog = MagicMock()
    dest_table = MagicMock()
    catalog.begin_transaction.return_value.__enter__.return_value.load_table.return_value = MagicMock()

    _apply_changes(catalog, dest_table, batch, ["id"], projection=projection)

    assert projection.apply.call_count == 0, "delete-only batch should not invoke projection"


def test_append_only_projection_resolved_per_attempt():
    """Same as apply_full_cdc: append_only must call projection_for inside the
    retry lambda so evict+rebuild picks up the fresh plan.
    """
    import pyarrow as pa

    from viaduck.apply import append_only

    pool = MagicMock()
    mock_table = MagicMock()
    pool.get.return_value = (MagicMock(), mock_table)
    pool.projection_for.return_value = None

    batch = pa.table({"a": [1], "b": ["x"]})

    attempts = {"count": 0}

    def _append_side_effect(_b):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise RuntimeError("first attempt fails")

    mock_table.append.side_effect = _append_side_effect

    with patch("viaduck.apply.time.sleep"):
        append_only(pool, "dest-1", batch)

    assert pool.projection_for.call_count == attempts["count"] == 2, (
        "append_only closure must also resolve projection per attempt"
    )


# ---------------------------------------------------------------------------
# _poll_cycle helpers
# ---------------------------------------------------------------------------


def _make_cfg(dest_ids_and_rvs: list[tuple[str, str]]):
    """Create a mock config with given (dest_id, routing_value) pairs."""
    cfg = MagicMock()
    dests = []
    for did, rv in dest_ids_and_rvs:
        d = MagicMock()
        d.id = did
        d.routing_value = rv
        dests.append(d)
    cfg.destinations = dests
    cfg.poll.cdc_chunk_snapshots = 100
    # Real values (not MagicMock auto-attrs): the cycle time-budget logic
    # compares these against the clock. Budget DISABLED by default here so a
    # slow CI box can never time-box mid-test; timebox tests opt in via
    # _timebox_setup.
    cfg.poll.interval_seconds = 5.0
    cfg.poll.cycle_time_budget_seconds = -1.0

    def by_id(dest_id):
        for d in dests:
            if d.id == dest_id:
                return d
        raise KeyError(dest_id)

    cfg.destination_by_id = by_id
    return cfg


# ---------------------------------------------------------------------------
# _poll_cycle (buffered delivery: reads + routing land in the DeliveryManager;
# writes are worker-side and covered by tests/unit/test_delivery.py)
# ---------------------------------------------------------------------------


def _make_delivery(positions: dict[str, int]):
    """Mock DeliveryManager: position map + status snapshot for all dests."""
    from viaduck.delivery import DestDeliveryStatus

    delivery = MagicMock()
    delivery.positions.return_value = dict(positions)
    delivery.flushed_snapshots.return_value = dict(positions)
    delivery.read_plan.return_value = {d: (snap, 0) for d, snap in positions.items()}
    delivery.should_pause_all_reads.return_value = False
    delivery.should_pause_reads_for.return_value = False
    delivery.maybe_flush.return_value = 0
    delivery.status_snapshot.return_value = {
        d: DestDeliveryStatus(
            flushed_snapshot=snap,
            position_snapshot=snap,
            rows_replicated=0,
            last_error=None,
            buffer_rows=0,
            buffer_age_s=0.0,
            flushing=False,
        )
        for d, snap in positions.items()
    }
    return delivery


def test_poll_cycle_no_snapshots():
    """If source has no snapshots, no reads happen — but triggers are still
    evaluated (position-only persists may be due)."""
    delivery = _make_delivery({})
    router = MagicMock()
    cfg = _make_cfg([])

    with patch("viaduck.main.source.snapshot_bounds", return_value=None):
        _poll_cycle(MagicMock(), delivery, MagicMock(), router, cfg, [], {}, key_columns=[], mode="append_only")

    delivery.read_plan.assert_not_called()
    delivery.maybe_flush.assert_called_once()


def test_poll_cycle_all_caught_up():
    """If every position is at the current snapshot, no CDC reads occur."""
    delivery = _make_delivery({"dest-1": 10})
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])

    with patch("viaduck.main.source.snapshot_bounds", return_value=(1, 10)):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1"],
            {"quacksworth": "dest-1"},
            key_columns=[],
            mode="append_only",
        )

    router.build_filter_expr.assert_not_called()
    delivery.buffer.assert_not_called()
    delivery.maybe_flush.assert_called_once()


def test_poll_cycle_routes_and_buffers():
    """Read CDC, route, buffer (BufferRead) — no synchronous writes."""
    delivery = _make_delivery({"dest-1": 5})
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])

    arrow_data = pa.table({"company": ["quacksworth", "quacksworth"], "value": [10, 20]})
    router.build_filter_expr.return_value = "company IN ('quacksworth')"
    router.split_and_count.return_value = ({"quacksworth": arrow_data}, 0)

    with (
        patch("viaduck.main.source.snapshot_bounds", return_value=(1, 10)),
        patch("viaduck.main.source.read_cdc", return_value=arrow_data),
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1"],
            {"quacksworth": "dest-1"},
            key_columns=[],
            mode="append_only",
        )

    delivery.buffer.assert_called_once_with("dest-1", arrow_data, 10, epoch=0)
    delivery.advance_position.assert_not_called()
    assert delivery.maybe_flush.call_count == 2  # once per chunk + once at end of cycle


def test_poll_cycle_empty_changeset_advances_positions():
    """An empty CDC range advances in-memory positions (no PG write)."""
    delivery = _make_delivery({"dest-1": 5, "dest-2": 5})
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "a"), ("dest-2", "b")])

    empty = pa.table({"company": pa.array([], type=pa.string())})
    with (
        patch("viaduck.main.source.snapshot_bounds", return_value=(1, 10)),
        patch("viaduck.main.source.read_cdc", return_value=empty),
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1", "dest-2"],
            {"a": "dest-1", "b": "dest-2"},
            key_columns=[],
            mode="append_only",
        )

    assert delivery.advance_position.call_count == 2
    delivery.advance_position.assert_any_call("dest-1", 10, epoch=0)
    delivery.advance_position.assert_any_call("dest-2", 10, epoch=0)
    delivery.buffer.assert_not_called()
    assert delivery.maybe_flush.call_count == 2  # once per chunk + once at end of cycle


def test_poll_cycle_skip_scan_fast_forwards_span_without_read():
    """A span with no source-table changes advances cursors without a CDC read."""
    delivery = _make_delivery({"dest-1": 5})
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "a")])

    with (
        patch("viaduck.main.source.snapshot_bounds", return_value=(1, 10)),
        patch("viaduck.main.source.next_snapshot_touching_table", return_value=None),
        patch("viaduck.main.source.read_cdc") as read_cdc,
        patch.object(metrics, "cdc_snapshots_skipped_total") as skipped,
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1"],
            {"a": "dest-1"},
            key_columns=[],
            mode="append_only",
        )

    read_cdc.assert_not_called()
    delivery.advance_position.assert_called_once_with("dest-1", 10, epoch=0)
    skipped.inc.assert_called_once_with(5)  # (5, 10] fast-forwarded


def test_poll_cycle_skip_scan_jumps_to_next_touching_snapshot():
    """The read starts at the snapshot before the next source-table change;
    the dead span in front of it advances cursor-only."""
    delivery = _make_delivery({"dest-1": 5})
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "a")])

    empty = pa.table({"company": pa.array([], type=pa.string())})
    with (
        patch("viaduck.main.source.snapshot_bounds", return_value=(1, 10)),
        patch("viaduck.main.source.next_snapshot_touching_table", return_value=9),
        patch("viaduck.main.source.read_cdc", return_value=empty) as read_cdc,
        patch.object(metrics, "cdc_snapshots_skipped_total") as skipped,
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1"],
            {"a": "dest-1"},
            key_columns=[],
            mode="append_only",
        )

    # Cursor-only advance over (5, 8], then a normal read of (8, 10].
    delivery.advance_position.assert_any_call("dest-1", 8, epoch=0)
    read_cdc.assert_called_once()
    assert read_cdc.call_args.kwargs["after_snapshot"] == 8
    assert read_cdc.call_args.kwargs["end_snapshot"] == 10
    skipped.inc.assert_called_once_with(3)


def test_poll_cycle_skip_scan_probe_failure_falls_back_to_sequential_read():
    """A broken probe must never stall the pipeline — reads proceed as before."""
    delivery = _make_delivery({"dest-1": 5})
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "a")])

    arrow_data = pa.table({"company": ["a"], "value": [1]})
    router.split_and_count.return_value = ({"a": arrow_data}, 0)
    with (
        patch("viaduck.main.source.snapshot_bounds", return_value=(1, 10)),
        patch("viaduck.main.source.next_snapshot_touching_table", side_effect=RuntimeError("catalog hiccup")),
        patch("viaduck.main.source.read_cdc", return_value=arrow_data) as read_cdc,
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1"],
            {"a": "dest-1"},
            key_columns=[],
            mode="append_only",
        )

    read_cdc.assert_called_once()
    assert read_cdc.call_args.kwargs["after_snapshot"] == 5
    delivery.buffer.assert_called_once()


def test_poll_cycle_routing_error_breaks_gracefully():
    """A routing failure stops reads for the cycle without buffering."""
    delivery = _make_delivery({"dest-1": 5})
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])

    arrow_data = pa.table({"other": ["x"]})
    router.split_and_count.side_effect = RoutingError("routing field missing")

    with (
        patch("viaduck.main.source.snapshot_bounds", return_value=(1, 10)),
        patch("viaduck.main.source.read_cdc", return_value=arrow_data),
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1"],
            {"quacksworth": "dest-1"},
            key_columns=[],
            mode="append_only",
        )

    delivery.buffer.assert_not_called()
    assert delivery.maybe_flush.call_count == 1  # routing error skips per-chunk flush


def test_poll_cycle_chunks_large_range():
    """Range larger than cdc_chunk_snapshots is split into multiple reads."""
    delivery = _make_delivery({"dest-1": 0})
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])
    cfg.poll.cdc_chunk_snapshots = 5  # positions 0→5, 5→10

    arrow_data = pa.table({"company": ["quacksworth"], "value": [1]})
    router.build_filter_expr.return_value = None
    router.split_and_count.return_value = ({"quacksworth": arrow_data}, 0)

    read_calls = []

    def fake_read_cdc(src_table, *, after_snapshot, end_snapshot, filter_expr=None):
        read_calls.append((after_snapshot, end_snapshot))
        return arrow_data

    with (
        patch("viaduck.main.source.snapshot_bounds", return_value=(1, 10)),
        patch("viaduck.main.source.read_cdc", side_effect=fake_read_cdc),
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1"],
            {"quacksworth": "dest-1"},
            key_columns=[],
            mode="append_only",
        )

    assert read_calls == [(0, 5), (5, 10)], f"expected two chunk reads, got {read_calls}"
    # cursor advances to chunk_end (5, then 10), not directly to 10 in one shot
    buffer_calls = delivery.buffer.call_args_list
    assert len(buffer_calls) == 2
    assert buffer_calls[0] == call("dest-1", arrow_data, 5, epoch=0)
    assert buffer_calls[1] == call("dest-1", arrow_data, 10, epoch=0)
    delivery.advance_position.assert_not_called()
    assert delivery.maybe_flush.call_count == 3  # once per chunk (×2) + once at end of cycle


def test_poll_cycle_chunk_end_not_current_id():
    """buffer() and advance_position() receive chunk_end, not current_id."""
    delivery = _make_delivery({"dest-1": 0, "dest-2": 0})
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "a"), ("dest-2", "b")])
    cfg.poll.cdc_chunk_snapshots = 3  # chunks: 0→3, 3→6, 6→7

    row_a = pa.table({"company": ["a"]})
    empty = pa.table({"company": pa.array([], type=pa.string())})
    call_count = [0]

    def fake_read(src_table, *, after_snapshot, end_snapshot, filter_expr=None):
        call_count[0] += 1
        return row_a if call_count[0] == 1 else empty

    router.build_filter_expr.return_value = None
    router.split_and_count.return_value = ({"a": row_a}, 0)

    with (
        patch("viaduck.main.source.snapshot_bounds", return_value=(1, 7)),
        patch("viaduck.main.source.read_cdc", side_effect=fake_read),
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1", "dest-2"],
            {"a": "dest-1", "b": "dest-2"},
            key_columns=[],
            mode="append_only",
        )

    # First chunk (0→3): dest-1 buffered at 3, dest-2 advanced to 3
    delivery.buffer.assert_any_call("dest-1", row_a, 3, epoch=0)
    delivery.advance_position.assert_any_call("dest-2", 3, epoch=0)
    # Chunks 3→6 and 6→7: empty, both advanced through to 7
    delivery.advance_position.assert_any_call("dest-1", 7, epoch=0)
    delivery.advance_position.assert_any_call("dest-2", 7, epoch=0)
    assert delivery.maybe_flush.call_count == 4  # once per chunk (×3) + once at end of cycle


def test_poll_cycle_multi_chunk_all_empty_flushes_per_chunk():
    """Multi-chunk catch-up where every chunk is empty still flushes and advances per chunk."""
    delivery = _make_delivery({"dest-1": 0})
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])
    cfg.poll.cdc_chunk_snapshots = 5  # two chunks: 0→5, 5→10

    empty = pa.table({"company": pa.array([], type=pa.string())})

    with (
        patch("viaduck.main.source.snapshot_bounds", return_value=(1, 10)),
        patch("viaduck.main.source.read_cdc", return_value=empty),
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1"],
            {"quacksworth": "dest-1"},
            key_columns=[],
            mode="append_only",
        )

    # cursor advanced to each chunk_end incrementally, not directly to 10
    assert delivery.advance_position.call_count == 2
    delivery.advance_position.assert_any_call("dest-1", 5, epoch=0)
    delivery.advance_position.assert_any_call("dest-1", 10, epoch=0)
    delivery.buffer.assert_not_called()
    assert delivery.maybe_flush.call_count == 3  # once per chunk (×2) + once at end of cycle


def test_poll_cycle_advances_no_data_destinations():
    """Destinations in the group with no routed rows advance positions."""
    delivery = _make_delivery({"dest-1": 5, "dest-2": 5})
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "a"), ("dest-2", "b")])

    arrow_data = pa.table({"company": ["a"], "value": [1]})
    router.split_and_count.return_value = ({"a": arrow_data}, 0)

    with (
        patch("viaduck.main.source.snapshot_bounds", return_value=(1, 10)),
        patch("viaduck.main.source.read_cdc", return_value=arrow_data),
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1", "dest-2"],
            {"a": "dest-1", "b": "dest-2"},
            key_columns=[],
            mode="append_only",
        )

    delivery.buffer.assert_called_once_with("dest-1", arrow_data, 10, epoch=0)
    delivery.advance_position.assert_called_once_with("dest-2", 10, epoch=0)
    assert delivery.maybe_flush.call_count == 2  # once per chunk + once at end of cycle


def test_poll_cycle_pauses_reads_at_watermark():
    """When every destination is at its buffer cap, the cycle skips reads
    entirely but still evaluates flush triggers."""
    delivery = _make_delivery({"dest-1": 5})
    delivery.should_pause_all_reads.return_value = True
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])

    with patch("viaduck.main.source.snapshot_bounds", return_value=(1, 10)):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1"],
            {"quacksworth": "dest-1"},
            key_columns=[],
            mode="append_only",
        )

    delivery.read_plan.assert_not_called()
    delivery.maybe_flush.assert_called_once()


def test_poll_cycle_mid_chunk_watermark_flushes_completed_chunks():
    """Watermark firing mid-loop only skips future chunks; completed chunks already flushed."""
    delivery = _make_delivery({"dest-1": 0})
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])
    cfg.poll.cdc_chunk_snapshots = 5  # chunks: 0→5 (completes), 5→10 (paused by watermark)

    empty = pa.table({"company": pa.array([], type=pa.string())})
    router.build_filter_expr.return_value = None

    # One cap check per destination per chunk iteration:
    # chunk-1 check False (read 0→5); chunk-2 check True (pause).
    delivery.should_pause_reads_for.side_effect = [False, True]

    with (
        patch("viaduck.main.source.snapshot_bounds", return_value=(1, 10)),
        patch("viaduck.main.source.read_cdc", return_value=empty),
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1"],
            {"quacksworth": "dest-1"},
            key_columns=[],
            mode="append_only",
        )

    # chunk 0→5 completed: cursor advanced and flushed
    delivery.advance_position.assert_called_once_with("dest-1", 5, epoch=0)
    # chunk 5→10 was never read (watermark break)
    assert delivery.maybe_flush.call_count == 2  # once after chunk 0→5 + once at end of cycle


def test_poll_cycle_reads_lowest_cursor_group_first():
    """Under buffer-watermark pressure, the read budget must go to the
    most-lagging cursor group first. Otherwise a caught-up destination fills
    the buffer before the lagging one gets its turn and the lagging group
    is silently starved every cycle (empirically observed on team-50689 with
    team-2 at lag 969 vs team-50689 at lag 7969 — team-50689 got zero
    reads for the entire pod lifetime).
    """
    # Two destinations at very different cursors: dest-lagging at 100,
    # dest-caughtup at 900. Config order lists dest-caughtup FIRST — the
    # bug (pre-fix) would iterate that group first, fill the buffer on
    # its read, and never get to dest-lagging.
    delivery = _make_delivery({"dest-lagging": 100, "dest-caughtup": 900})
    router = MagicMock()
    cfg = _make_cfg([("dest-caughtup", "cup"), ("dest-lagging", "lag")])
    cfg.poll.cdc_chunk_snapshots = 200

    # One cap check per destination per chunk iteration. Caps trip after
    # the FIRST chunk read: lagging group's chunk-1 check False (one read),
    # chunk-2 check True (cap filled); the caught-up group's chunk-1 check
    # True (also at cap). Whichever group iterates first gets the only read.
    delivery.should_pause_reads_for.side_effect = [False, True, True]

    read_calls: list[tuple[int, int]] = []

    def fake_read(src_table, *, after_snapshot, end_snapshot, filter_expr=None):
        read_calls.append((after_snapshot, end_snapshot))
        return pa.table({"company": pa.array([], type=pa.string())})

    router.build_filter_expr.return_value = None

    with (
        patch("viaduck.main.source.snapshot_bounds", return_value=(1, 1000)),
        patch("viaduck.main.source.read_cdc", side_effect=fake_read),
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-caughtup", "dest-lagging"],
            {"cup": "dest-caughtup", "lag": "dest-lagging"},
            key_columns=[],
            mode="append_only",
        )

    # Only one read should have happened (the second group's chunk was
    # paused). It MUST have been the lagging group (starting at 100), not
    # the caught-up group (starting at 900).
    assert len(read_calls) == 1, f"expected exactly one read before watermark trip, got {read_calls}"
    assert read_calls[0][0] == 100, (
        f"lagging cursor should have read first; instead read started at {read_calls[0][0]} "
        f"(the config-order-first, caught-up destination)"
    )


def test_poll_cycle_stuck_destination_at_cap_does_not_block_healthy_peer():
    """The tonight-in-prod regression this whole change exists for: a
    lagging destination whose queue is pinned at its per-destination cap
    (failing flushes hold its in-flight bytes) must NOT prevent a healthy
    peer's group from reading. Under the old GLOBAL watermark, the stuck
    destination's bytes tripped the shared check and the healthy peer's
    first chunk check bounced it out with zero reads — team-2's cursor
    froze for 1.5h while team-50689 held ~250MB in flight.
    """
    delivery = _make_delivery({"dest-stuck": 100, "dest-healthy": 900})
    router = MagicMock()
    cfg = _make_cfg([("dest-stuck", "stk"), ("dest-healthy", "ok")])
    cfg.poll.cdc_chunk_snapshots = 200

    # dest-stuck is at ITS cap (queue full, flush failing); dest-healthy
    # has headroom. Per-destination gating: stuck's group skips, healthy's
    # group reads.
    delivery.should_pause_reads_for.side_effect = lambda d: d == "dest-stuck"

    read_calls: list[tuple[int, int]] = []

    def fake_read(src_table, *, after_snapshot, end_snapshot, filter_expr=None):
        read_calls.append((after_snapshot, end_snapshot))
        return pa.table({"company": pa.array([], type=pa.string())})

    router.build_filter_expr.return_value = None

    with (
        patch("viaduck.main.source.snapshot_bounds", return_value=(1, 1000)),
        patch("viaduck.main.source.read_cdc", side_effect=fake_read),
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-stuck", "dest-healthy"],
            {"stk": "dest-stuck", "ok": "dest-healthy"},
            key_columns=[],
            mode="append_only",
        )

    # The healthy destination read its range; the stuck one was skipped.
    assert read_calls == [(900, 1000)], (
        f"healthy peer must read while the stuck destination sits at its cap, got {read_calls}"
    )


def test_poll_cycle_mixed_group_reads_only_members_with_headroom():
    """QE must-fix #2 + both principal-SWE HIGHs: two destinations share a
    cursor group; one is at its cap (slow/hung flush holding its in-flight
    bytes), the other has headroom. The chunk read must proceed for the
    healthy member ONLY: the at-cap member's routing value is excluded from
    the CDC filter and its position stays frozen (it splits into its own
    group next cycle). Under the earlier all()-gate + unconditional
    buffer(), the at-cap member kept receiving its split slice on the
    peer's headroom — unbounded growth for as long as its flush hung,
    with no global watermark left to backstop it.
    """
    delivery = _make_delivery({"dest-full": 100, "dest-ok": 100})  # same cursor → one group
    router = MagicMock()
    cfg = _make_cfg([("dest-full", "full"), ("dest-ok", "ok")])
    cfg.poll.cdc_chunk_snapshots = 200

    delivery.should_pause_reads_for.side_effect = lambda d: d == "dest-full"

    arrow_data = pa.table({"company": ["ok"], "value": [1]})
    router.split_and_count.return_value = ({"ok": arrow_data}, 0)

    filter_calls: list[list[str]] = []

    def fake_filter(routing_values):
        filter_calls.append(list(routing_values))
        return "company IN (...)"

    router.build_filter_expr.side_effect = fake_filter

    with (
        patch("viaduck.main.source.snapshot_bounds", return_value=(1, 300)),
        patch("viaduck.main.source.read_cdc", return_value=arrow_data),
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-full", "dest-ok"],
            {"full": "dest-full", "ok": "dest-ok"},
            key_columns=[],
            mode="append_only",
        )

    # The CDC filter only ever asked for the healthy member's rows.
    assert filter_calls and all(c == ["ok"] for c in filter_calls), (
        f"at-cap member's routing value must be excluded from the read filter, got {filter_calls}"
    )
    # Healthy member buffered; at-cap member neither buffered nor advanced.
    buffered_dests = {c.args[0] for c in delivery.buffer.call_args_list}
    assert buffered_dests == {"dest-ok"}
    advanced_dests = {c.args[0] for c in delivery.advance_position.call_args_list}
    assert "dest-full" not in advanced_dests, (
        "at-cap member's position must stay frozen so it forms its own skippable group"
    )


def test_poll_cycle_lagging_group_cannot_monopolize_reads():
    """The per-cycle chunk cap turns lagging-first priority into round-robin:
    a deeply-lagging group reads at most _MAX_CHUNKS_PER_GROUP_PER_CYCLE
    chunks per cycle, then the healthy group gets its turn. Without the cap
    (the first sort-only fix), the lagging group's chunk loop ran to head —
    or forever when its flushes kept failing and its position kept resetting
    — and the healthy destination's cursor froze (observed on team-50689:
    team-2 starved for 1.5h while team-50689 relitigated the same range).
    """
    from viaduck.main import _MAX_CHUNKS_PER_GROUP_PER_CYCLE

    # dest-lagging is 10,000 snapshots behind; dest-healthy is 100 behind.
    # Uncapped, dest-lagging's group would issue 100 chunk reads before
    # dest-healthy saw a single one.
    delivery = _make_delivery({"dest-lagging": 0, "dest-healthy": 9_900})
    router = MagicMock()
    cfg = _make_cfg([("dest-lagging", "lag"), ("dest-healthy", "ok")])
    cfg.poll.cdc_chunk_snapshots = 100

    read_calls: list[tuple[int, int]] = []

    def fake_read(src_table, *, after_snapshot, end_snapshot, filter_expr=None):
        read_calls.append((after_snapshot, end_snapshot))
        return pa.table({"company": pa.array([], type=pa.string())})

    router.build_filter_expr.return_value = None

    with (
        patch("viaduck.main.source.snapshot_bounds", return_value=(1, 10_000)),
        patch("viaduck.main.source.read_cdc", side_effect=fake_read),
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-lagging", "dest-healthy"],
            {"lag": "dest-lagging", "ok": "dest-healthy"},
            key_columns=[],
            mode="append_only",
        )

    # The lagging group read exactly the cap, no more.
    lagging_reads = [c for c in read_calls if c[0] < 9_900]
    assert len(lagging_reads) == _MAX_CHUNKS_PER_GROUP_PER_CYCLE, (
        f"lagging group should read exactly the per-cycle cap, got {len(lagging_reads)}: {lagging_reads}"
    )
    # The healthy group STILL got its read this cycle — the load-bearing
    # assertion. Under the uncapped loop this list is empty.
    healthy_reads = [c for c in read_calls if c[0] >= 9_900]
    assert healthy_reads == [(9_900, 10_000)], f"healthy group must read within the same cycle, got {healthy_reads}"
    # Order: lagging first (priority preserved), healthy after.
    assert read_calls[0][0] == 0 and read_calls[-1][0] == 9_900


def test_poll_cycle_snapshot_at_zero():
    """Source snapshot 0 with positions at 0: nothing to read, triggers run."""
    delivery = _make_delivery({"dest-1": 0})
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])

    with patch("viaduck.main.source.snapshot_bounds", return_value=(1, 0)):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1"],
            {"quacksworth": "dest-1"},
            key_columns=[],
            mode="append_only",
        )

    router.build_filter_expr.assert_not_called()
    delivery.maybe_flush.assert_called_once()


# ---------------------------------------------------------------------------
# _resolve_preimages
# ---------------------------------------------------------------------------


def _cdc_table(rows, routing_field="company"):
    """Build a pyarrow table with CDC metadata columns from list of dicts."""
    if not rows:
        return pa.table(
            {
                routing_field: pa.array([], type=pa.string()),
                "value": pa.array([], type=pa.int64()),
                "change_type": pa.array([], type=pa.string()),
                "snapshot_id": pa.array([], type=pa.int64()),
                "rowid": pa.array([], type=pa.int64()),
            }
        )
    cols = {}
    for key in rows[0]:
        cols[key] = [r[key] for r in rows]
    return pa.table(cols)


def test_resolve_preimages_same_tenant_drops():
    """Preimage with same routing value as postimage should be dropped."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "update_preimage", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 2, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 100},
        ]
    )
    result = _resolve_preimages(batch, "company", ["value"])
    assert result.num_rows == 1
    assert result.column("change_type")[0].as_py() == "update_postimage"


def test_resolve_preimages_cross_tenant_converts_to_delete():
    """Different routing values: preimage becomes delete. Metric incremented."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "update_preimage", "snapshot_id": 1, "rowid": 100},
            {"company": "beta", "value": 2, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 100},
        ]
    )
    with patch("viaduck.main.metrics.cdc_routing_mutations_total") as mock_metric:
        result = _resolve_preimages(batch, "company", ["value"])
    assert result.num_rows == 2
    types = result.column("change_type").to_pylist()
    assert types[0] == "delete"
    assert types[1] == "update_postimage"
    mock_metric.inc.assert_called_once()


def test_resolve_preimages_orphaned_converts_to_delete():
    """Preimage with no matching postimage becomes delete. Metric incremented."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "update_preimage", "snapshot_id": 1, "rowid": 100},
            {"company": "beta", "value": 2, "change_type": "insert", "snapshot_id": 1, "rowid": 200},
        ]
    )
    with patch("viaduck.main.metrics.cdc_orphaned_preimages_total") as mock_metric:
        result = _resolve_preimages(batch, "company", ["value"])
    assert result.num_rows == 2
    types = result.column("change_type").to_pylist()
    assert types[0] == "delete"
    assert types[1] == "insert"
    mock_metric.inc.assert_called_once()


def test_resolve_preimages_no_preimages():
    """Batch with no preimages should pass through unchanged."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "beta", "value": 2, "change_type": "delete", "snapshot_id": 1, "rowid": 200},
        ]
    )
    result = _resolve_preimages(batch, "company", ["value"])
    assert result.num_rows == 2
    assert result.column("change_type").to_pylist() == ["insert", "delete"]


def test_resolve_preimages_mixed_same_and_cross():
    """Mix of same-tenant and cross-tenant updates."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "update_preimage", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 2, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 100},
            {"company": "old", "value": 3, "change_type": "update_preimage", "snapshot_id": 1, "rowid": 200},
            {"company": "new", "value": 4, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 200},
        ]
    )
    result = _resolve_preimages(batch, "company", ["value"])
    # Same-tenant (rowid=100): preimage dropped -> postimage only
    # Cross-tenant (rowid=200): preimage -> delete, postimage kept
    assert result.num_rows == 3
    types = result.column("change_type").to_pylist()
    assert types == ["update_postimage", "delete", "update_postimage"]


def test_resolve_preimages_preserves_non_update_rows():
    """Inserts and deletes pass through unmodified."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "beta", "value": 2, "change_type": "delete", "snapshot_id": 1, "rowid": 200},
        ]
    )
    result = _resolve_preimages(batch, "company", ["value"])
    assert result.num_rows == 2
    assert result.column("change_type").to_pylist() == ["insert", "delete"]
    assert result.column("value").to_pylist() == [1, 2]


def test_resolve_preimages_validates_key_columns_exist():
    """Missing key column should raise RoutingError."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
        ]
    )
    with pytest.raises(RoutingError, match="Key column 'missing_col' not found"):
        _resolve_preimages(batch, "company", ["missing_col"])


# ---------------------------------------------------------------------------
# _resolve_conflicts
# ---------------------------------------------------------------------------


def test_resolve_conflicts_insert_delete_keeps_tombstone():
    """Same rowid insert + delete: the insert drops, the delete SURVIVES
    (tombstone). Against a destination that never saw the insert it is an
    idempotent no-op; against one that did (commit/cursor-gap replay) it
    is the only event that can remove the phantom. Both metrics fire."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 1, "change_type": "delete", "snapshot_id": 1, "rowid": 100},
        ]
    )
    with (
        patch("viaduck.main.metrics.cdc_conflicts_resolved_total") as mock_conflicts,
        patch("viaduck.main.metrics.cdc_tombstones_emitted_total") as mock_tombstones,
    ):
        result = _resolve_conflicts(batch)
    assert result.column("change_type").to_pylist() == ["delete"]
    assert result.column("rowid").to_pylist() == [100]
    mock_conflicts.inc.assert_called_once_with(1)
    mock_tombstones.inc.assert_called_once_with(1)


def test_resolve_conflicts_update_delete_keeps_delete():
    """Same rowid postimage + delete: postimage dropped, delete kept."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 2, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 1, "change_type": "delete", "snapshot_id": 1, "rowid": 100},
        ]
    )
    result = _resolve_conflicts(batch)
    assert result.num_rows == 1
    assert result.column("change_type")[0].as_py() == "delete"


def test_resolve_conflicts_no_conflicts():
    """No overlapping rowids: unchanged."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "beta", "value": 2, "change_type": "insert", "snapshot_id": 1, "rowid": 200},
        ]
    )
    result = _resolve_conflicts(batch)
    assert result.num_rows == 2


def test_resolve_conflicts_mixed_pairs_and_plain():
    """A paired rowid keeps only its tombstone delete; unrelated rows pass."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 1, "change_type": "delete", "snapshot_id": 1, "rowid": 100},
            {"company": "beta", "value": 2, "change_type": "insert", "snapshot_id": 1, "rowid": 200},
        ]
    )
    result = _resolve_conflicts(batch)
    by_rowid = dict(zip(result.column("rowid").to_pylist(), result.column("change_type").to_pylist()))
    assert by_rowid == {100: "delete", 200: "insert"}


def test_resolve_conflicts_empty_batch():
    """Empty batch should return empty."""
    batch = _cdc_table([])
    result = _resolve_conflicts(batch)
    assert result.num_rows == 0


def test_resolve_conflicts_insert_update_delete_sequence():
    """Same rowid: insert + postimage + delete. Insert and postimage drop;
    the delete survives as the tombstone (matches spec Phase2)."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 2, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 1, "change_type": "delete", "snapshot_id": 1, "rowid": 100},
        ]
    )
    result = _resolve_conflicts(batch)
    assert result.column("change_type").to_pylist() == ["delete"]


def test_resolve_conflicts_duplicate_keys_last_wins():
    """Multiple inserts for same rowid: verify no crash."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 2, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
        ]
    )
    # No crash expected, both rows preserved (no delete to trigger cancellation)
    result = _resolve_conflicts(batch)
    assert result.num_rows == 2


def test_resolve_conflicts_same_key_different_rowid_no_cancel():
    """Same key_columns value but different rowid should NOT cancel."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 1, "change_type": "delete", "snapshot_id": 1, "rowid": 200},
        ]
    )
    result = _resolve_conflicts(batch)
    # Different rowids, no cancellation
    assert result.num_rows == 2


def test_resolve_conflicts_uses_rowid_not_just_key():
    """Explicit test that rowid is used for matching, not key column values."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 1, "change_type": "delete", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 300},
        ]
    )
    result = _resolve_conflicts(batch)
    # rowid 100: insert drops, tombstone delete survives. rowid 300: insert preserved.
    by_rowid = dict(zip(result.column("rowid").to_pylist(), result.column("change_type").to_pylist()))
    assert by_rowid == {100: "delete", 300: "insert"}


def test_resolve_conflicts_insert_postimage_same_rowid_drops_insert():
    """Same rowid with insert + update_postimage: drop the insert, keep postimage.

    Repro for the flaky CI failure in tests/integration::test_full_cdc_update_round_trip.
    When the source's table_changes range covers an INSERT and a later same-rowid
    UPDATE (because the upsert reused the rowid rather than delete+insert), Phase 1
    drops the same-tenant preimage, leaving INSERT(rowid=R, value=old) and
    UPDATE_POSTIMAGE(rowid=R, value=new) for the same key. Phase 3 _apply_changes
    feeds both rows into a single tbl.upsert(join_cols=...) — which has undefined
    ordering for duplicate join keys, so the older value can win non-deterministically.

    Phase 2 must collapse this pair: the postimage represents the newer state.
    """
    batch = _cdc_table(
        [
            {"company": "acme", "value": 10, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 999, "change_type": "update_postimage", "snapshot_id": 2, "rowid": 100},
        ]
    )
    result = _resolve_conflicts(batch)
    assert result.num_rows == 1
    assert result.column("change_type")[0].as_py() == "update_postimage"
    assert result.column("value")[0].as_py() == 999


# ---------------------------------------------------------------------------
# _build_delete_filter
# ---------------------------------------------------------------------------


def test_build_delete_filter_single_key():
    """Single key column with multiple values produces IN expression."""
    rows = pa.table(
        {
            "id": [1, 2, 3],
            "change_type": ["delete", "delete", "delete"],
            "snapshot_id": [1, 1, 1],
            "rowid": [10, 20, 30],
        }
    )
    sql = _build_delete_filter(rows, ["id"])
    assert "IN" in sql
    assert "1" in sql
    assert "2" in sql
    assert "3" in sql


def test_build_delete_filter_composite_key():
    """Composite key produces OR(AND(...), AND(...))."""
    rows = pa.table(
        {
            "a": [1, 2],
            "b": ["x", "y"],
            "change_type": ["delete", "delete"],
            "snapshot_id": [1, 1],
            "rowid": [10, 20],
        }
    )
    sql = _build_delete_filter(rows, ["a", "b"])
    assert "OR" in sql or "AND" in sql


def test_build_delete_filter_single_row():
    """Single row produces simple equality."""
    rows = pa.table(
        {
            "id": [42],
            "change_type": ["delete"],
            "snapshot_id": [1],
            "rowid": [10],
        }
    )
    sql = _build_delete_filter(rows, ["id"])
    assert "42" in sql


def test_build_delete_filter_null_in_key():
    """NULL value in key should use IS NULL."""
    rows = pa.table(
        {
            "id": pa.array([None, 1], type=pa.int64()),
            "change_type": ["delete", "delete"],
            "snapshot_id": [1, 1],
            "rowid": [10, 20],
        }
    )
    sql = _build_delete_filter(rows, ["id"])
    assert "NULL" in sql.upper()


def test_build_delete_filter_all_null_composite_key():
    """All NULLs in composite key."""
    rows = pa.table(
        {
            "a": pa.array([None], type=pa.int64()),
            "b": pa.array([None], type=pa.string()),
            "change_type": ["delete"],
            "snapshot_id": [1],
            "rowid": [10],
        }
    )
    sql = _build_delete_filter(rows, ["a", "b"])
    assert "NULL" in sql.upper()


def test_build_delete_filter_composite_key_partial_null_column():
    """Composite key where only one column has nulls — exercises col_has_nulls short-circuit."""
    rows = pa.table(
        {
            "a": pa.array([None, 2], type=pa.int64()),  # has nulls
            "b": pa.array(["x", "y"], type=pa.string()),  # no nulls
            "change_type": ["delete", "delete"],
            "snapshot_id": [1, 2],
            "rowid": [10, 20],
        }
    )
    sql = _build_delete_filter(rows, ["a", "b"])
    # First row: a IS NULL AND b = 'x'
    # Second row: a = 2 AND b = 'y'
    assert "NULL" in sql.upper()
    assert "2" in sql
    assert "x" in sql
    assert "y" in sql


def test_build_delete_filter_mixed_null_and_values():
    """Mix of NULL and non-NULL for single key column."""
    rows = pa.table(
        {
            "id": pa.array([None, 5, None, 7], type=pa.int64()),
            "change_type": ["delete"] * 4,
            "snapshot_id": [1] * 4,
            "rowid": [10, 20, 30, 40],
        }
    )
    sql = _build_delete_filter(rows, ["id"])
    assert "NULL" in sql.upper()
    assert "IN" in sql or "5" in sql


def test_build_delete_filter_missing_key_column_raises():
    """Missing key column should raise RoutingError."""
    rows = pa.table(
        {
            "id": [1],
            "change_type": ["delete"],
            "snapshot_id": [1],
            "rowid": [10],
        }
    )
    with pytest.raises(RoutingError, match="Key column 'missing' not found"):
        _build_delete_filter(rows, ["missing"])


# ---------------------------------------------------------------------------
# _apply_changes
# ---------------------------------------------------------------------------


def _mock_catalog_and_table():
    """Create mock catalog with transaction context manager and table."""
    catalog = MagicMock()
    dest_table = MagicMock()
    dest_table.identifier = "test_table"
    txn = MagicMock()
    txn_table = MagicMock()
    # Mock upsert to return UpsertResult-like object
    upsert_result = MagicMock()
    upsert_result.rows_updated = 0
    upsert_result.rows_inserted = 0
    txn_table.upsert.return_value = upsert_result
    txn.load_table.return_value = txn_table
    catalog.begin_transaction.return_value.__enter__ = MagicMock(return_value=txn)
    catalog.begin_transaction.return_value.__exit__ = MagicMock(return_value=False)
    return catalog, dest_table, txn, txn_table


def test_apply_changes_inserts_only():
    """Only inserts: upsert called, no delete.

    These _apply_changes tests key on "value" (unique per fixture row), not
    "company" (constant): with a realistic key, duplicate-key rows collapse
    via Winner(k) and the pass-through counts asserted here would change.
    Same-key behavior is covered by the Winner(k) tests below."""
    catalog, dest_table, txn, txn_table = _mock_catalog_and_table()
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 2, "change_type": "insert", "snapshot_id": 1, "rowid": 200},
        ]
    )
    counts = _apply_changes(catalog, dest_table, batch, ["value"])
    assert counts["upserted"] == 2
    assert counts["deleted"] == 0
    txn_table.upsert.assert_called_once()
    assert txn_table.upsert.call_args.kwargs["join_cols"] == ["value"]
    txn_table.delete.assert_not_called()


def test_apply_changes_deletes_only():
    """Only deletes: delete called, no upsert."""
    catalog, dest_table, txn, txn_table = _mock_catalog_and_table()
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "delete", "snapshot_id": 1, "rowid": 100},
        ]
    )
    counts = _apply_changes(catalog, dest_table, batch, ["company"])
    assert counts["deleted"] == 1
    assert counts["upserted"] == 0
    txn_table.delete.assert_called_once()
    txn_table.upsert.assert_not_called()


def test_apply_changes_updates_only():
    """Postimages should be upserted."""
    catalog, dest_table, txn, txn_table = _mock_catalog_and_table()
    batch = _cdc_table(
        [
            {"company": "acme", "value": 5, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 100},
        ]
    )
    counts = _apply_changes(catalog, dest_table, batch, ["company"])
    assert counts["upserted"] == 1
    assert counts["deleted"] == 0
    txn_table.upsert.assert_called_once()
    assert txn_table.upsert.call_args.kwargs["join_cols"] == ["company"]


def test_apply_changes_mixed():
    """Mixed deletes and inserts: both called, correct counts."""
    catalog, dest_table, txn, txn_table = _mock_catalog_and_table()
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "delete", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 2, "change_type": "insert", "snapshot_id": 1, "rowid": 200},
            {"company": "acme", "value": 3, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 300},
        ]
    )
    counts = _apply_changes(catalog, dest_table, batch, ["value"])
    assert counts["deleted"] == 1
    assert counts["upserted"] == 2
    txn_table.delete.assert_called_once()
    txn_table.upsert.assert_called_once()
    assert txn_table.upsert.call_args.kwargs["join_cols"] == ["value"]


def test_apply_changes_empty():
    """Empty batch: no-op, no transaction."""
    catalog, dest_table, txn, txn_table = _mock_catalog_and_table()
    batch = _cdc_table([])
    counts = _apply_changes(catalog, dest_table, batch, ["company"])
    assert counts == {"deleted": 0, "upserted": 0, "upsert_matched": 0}
    catalog.begin_transaction.assert_not_called()


def test_apply_changes_uses_transaction():
    """begin_transaction should be called."""
    catalog, dest_table, txn, txn_table = _mock_catalog_and_table()
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
        ]
    )
    _apply_changes(catalog, dest_table, batch, ["company"])
    catalog.begin_transaction.assert_called_once()


def test_apply_changes_transaction_rollback_on_failure():
    """Exception inside transaction should propagate (context manager handles rollback)."""
    catalog, dest_table, txn, txn_table = _mock_catalog_and_table()
    txn_table.upsert.side_effect = Exception("write error")
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
        ]
    )
    with pytest.raises(Exception, match="write error"):
        _apply_changes(catalog, dest_table, batch, ["company"])


def test_apply_changes_strips_metadata():
    """change_type, snapshot_id, rowid should be removed before write."""
    catalog, dest_table, txn, txn_table = _mock_catalog_and_table()
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
        ]
    )
    _apply_changes(catalog, dest_table, batch, ["company"])
    upsert_call = txn_table.upsert.call_args
    written_table = upsert_call[0][0]
    assert "change_type" not in written_table.column_names
    assert "snapshot_id" not in written_table.column_names
    assert "rowid" not in written_table.column_names
    assert "company" in written_table.column_names
    assert "value" in written_table.column_names


# ---------------------------------------------------------------------------
# append_only: boundary check for accidentally-routed CDC batches
# ---------------------------------------------------------------------------


def test_append_only_accepts_insert_only_batch():
    """The expected source.read_cdc shape (ducklake_table_insertions output)
    has no `change_type` column — those rows flow straight through to
    tbl.append(). Sanity-check that the defensive guard doesn't reject the
    happy path."""
    pool = MagicMock()
    pool.get.return_value = (MagicMock(), MagicMock())
    pool.projection_for.return_value = None  # no schema projection for this destination
    batch = pa.table(
        {
            "company": ["acme"],
            "value": [1],
            # NOTE: snapshot_id/rowid are present (read_cdc returns them) but
            # `change_type` is NOT — that's the insert-only contract from
            # ducklake_table_insertions.
            "snapshot_id": [1],
            "rowid": [100],
        }
    )
    written = append_only(pool, "dest-1", batch)
    assert written == 1


def test_append_only_rejects_batch_with_change_type_column():
    """If a future viaduck change accidentally routes a ducklake_table_changes
    batch (which carries `change_type`) into the append path, the destination
    would silently land deletes and update_postimages as plain inserts. The
    boundary guard catches this and refuses to write."""
    pool = MagicMock()
    pool.get.return_value = (MagicMock(), MagicMock())
    batch = pa.table(
        {
            "company": ["acme", "acme"],
            "value": [1, 2],
            "change_type": ["insert", "delete"],
            "snapshot_id": [1, 1],
            "rowid": [100, 200],
        }
    )
    with pytest.raises(RuntimeError, match="change_type"):
        append_only(pool, "dest-1", batch)


def test_append_only_empty_batch_is_noop_even_with_change_type():
    """The fast no-op exit for empty batches runs BEFORE the boundary check.
    An empty batch with a `change_type` column (degenerate but possible from
    a filter that masked everything) is still a no-op rather than a noisy
    error — nothing to misclassify, nothing to write."""
    pool = MagicMock()
    batch = pa.table(
        {
            "company": pa.array([], type=pa.string()),
            "change_type": pa.array([], type=pa.string()),
        }
    )
    assert append_only(pool, "dest-1", batch) == 0
    pool.get.assert_not_called()


# ---------------------------------------------------------------------------
# _poll_cycle (full CDC mode)
# ---------------------------------------------------------------------------


def _make_real_delivery(state_mgr, dest_pool, key_columns, assigned_ids, *, mode):
    """Real DeliveryManager in flush-every-cycle mode so end-to-end poll
    tests keep their write coverage. wait_idle() joins the single worker.

    mode is keyword-only and required — the helper deliberately does NOT
    derive it from key_columns presence (that was the silent-misconfig
    hazard this PR removes from production; allowing it back in via a test
    helper would let a future "test the validation matrix" use case sneak
    through inverted)."""
    from viaduck.config import DeliveryConfig
    from viaduck.delivery import DeliveryManager

    dcfg = DeliveryConfig(workers=1, flush_interval_seconds=0.0)
    return DeliveryManager(dcfg, state_mgr, dest_pool, key_columns, assigned_ids, mode=mode)


def _txn_catalog():
    """Mock catalog whose begin_transaction context yields a txn table."""
    mock_catalog = MagicMock()
    txn = MagicMock()
    txn_table = MagicMock()
    upsert_result = MagicMock()
    upsert_result.rows_updated = 0
    upsert_result.rows_inserted = 0
    txn_table.upsert.return_value = upsert_result
    txn.load_table.return_value = txn_table
    mock_catalog.begin_transaction.return_value.__enter__ = MagicMock(return_value=txn)
    mock_catalog.begin_transaction.return_value.__exit__ = MagicMock(return_value=False)
    return mock_catalog, txn_table


def _cursor(snapshot=5, rows=0):
    c = MagicMock()
    c.last_snapshot_id = snapshot
    c.rows_replicated = rows
    c.last_error = None
    return c


def test_poll_cycle_full_cdc_routes_and_writes():
    """Full CDC end-to-end: read, resolve preimages, route, buffer, flush
    (conflict resolution + apply on the worker), advance cursor."""
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    dest_pool.projection_for.return_value = None
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])
    cfg.routing.field = "company"

    state_mgr.load_cursors.return_value = {"dest-1": _cursor(5, 100)}

    arrow_data = _cdc_table(
        [
            {"company": "quacksworth", "value": 10, "change_type": "insert", "snapshot_id": 6, "rowid": 1},
            {"company": "quacksworth", "value": 20, "change_type": "insert", "snapshot_id": 6, "rowid": 2},
        ]
    )
    router.build_filter_expr.return_value = "company IN ('quacksworth')"
    router.split_and_count.return_value = ({"quacksworth": arrow_data}, 0)

    mock_catalog, txn_table = _txn_catalog()
    mock_dest_table = MagicMock()
    mock_dest_table.identifier = "dest_table"
    mock_dest_table.scan.return_value.count.return_value = 0  # empty dest: no truncate
    dest_pool.get.return_value = (mock_catalog, mock_dest_table)

    delivery = _make_real_delivery(state_mgr, dest_pool, ["value"], ["dest-1"], mode="full_cdc")
    with (
        patch("viaduck.main.source.snapshot_bounds", return_value=(1, 10)),
        patch("viaduck.main.source.read_cdc_changes", return_value=arrow_data),
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            dest_pool,
            router,
            cfg,
            ["dest-1"],
            {"quacksworth": "dest-1"},
            key_columns=["value"],
            mode="full_cdc",
        )
    assert delivery.wait_idle()

    txn_table.upsert.assert_called_once()
    state_mgr.advance_cursor.assert_called_once_with("dest-1", 10, cumulative_rows=102)


def test_poll_cycle_append_only_unchanged():
    """Append-only mode uses read_cdc and table.append (on the worker)."""
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    dest_pool.projection_for.return_value = None
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])

    state_mgr.load_cursors.return_value = {"dest-1": _cursor(5, 0)}

    arrow_data = pa.table({"company": ["quacksworth"], "value": [10]})
    router.build_filter_expr.return_value = "company IN ('quacksworth')"
    router.split_and_count.return_value = ({"quacksworth": arrow_data}, 0)

    mock_dest_table = MagicMock()
    dest_pool.get.return_value = (MagicMock(), mock_dest_table)

    delivery = _make_real_delivery(state_mgr, dest_pool, [], ["dest-1"], mode="append_only")
    with (
        patch("viaduck.main.source.snapshot_bounds", return_value=(1, 10)),
        patch("viaduck.main.source.read_cdc", return_value=arrow_data) as mock_read_cdc,
        patch("viaduck.main.source.read_cdc_changes") as mock_read_changes,
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            dest_pool,
            router,
            cfg,
            ["dest-1"],
            {"quacksworth": "dest-1"},
            key_columns=[],
            mode="append_only",
        )
    assert delivery.wait_idle()

    mock_read_cdc.assert_called_once()
    mock_read_changes.assert_not_called()
    mock_dest_table.append.assert_called_once()
    state_mgr.advance_cursor.assert_called_once_with("dest-1", 10, cumulative_rows=1)


def test_poll_cycle_cdc_delete_only_changeset():
    """CDC mode with only deletes flows through the flush worker."""
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    dest_pool.projection_for.return_value = None
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])
    cfg.routing.field = "company"

    state_mgr.load_cursors.return_value = {"dest-1": _cursor(5, 50)}

    arrow_data = _cdc_table(
        [
            {"company": "quacksworth", "value": 1, "change_type": "delete", "snapshot_id": 6, "rowid": 1},
        ]
    )
    router.build_filter_expr.return_value = "company IN ('quacksworth')"
    router.split_and_count.return_value = ({"quacksworth": arrow_data}, 0)

    mock_catalog, txn_table = _txn_catalog()
    mock_dest_table = MagicMock()
    mock_dest_table.identifier = "dest_table"
    mock_dest_table.scan.return_value.count.return_value = 0  # empty dest: no truncate
    dest_pool.get.return_value = (mock_catalog, mock_dest_table)

    delivery = _make_real_delivery(state_mgr, dest_pool, ["company"], ["dest-1"], mode="full_cdc")
    with (
        patch("viaduck.main.source.snapshot_bounds", return_value=(1, 10)),
        patch("viaduck.main.source.read_cdc_changes", return_value=arrow_data),
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            dest_pool,
            router,
            cfg,
            ["dest-1"],
            {"quacksworth": "dest-1"},
            key_columns=["company"],
            mode="full_cdc",
        )
    assert delivery.wait_idle()

    txn_table.delete.assert_called_once()
    txn_table.upsert.assert_not_called()
    state_mgr.advance_cursor.assert_called_once_with("dest-1", 10, cumulative_rows=51)


def test_poll_cycle_cdc_write_failure_isolation():
    """Flush failure: error recorded, connection evicted, buffer dropped,
    position reset to the persisted cursor — and the process survives."""
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])
    cfg.routing.field = "company"

    state_mgr.load_cursors.return_value = {"dest-1": _cursor(5, 0)}

    arrow_data = _cdc_table(
        [
            {"company": "quacksworth", "value": 1, "change_type": "insert", "snapshot_id": 6, "rowid": 1},
        ]
    )
    router.build_filter_expr.return_value = "company IN ('quacksworth')"
    router.split_and_count.return_value = ({"quacksworth": arrow_data}, 0)

    mock_catalog = MagicMock()
    mock_dest_table = MagicMock()
    mock_dest_table.identifier = "dest_table"
    mock_catalog.begin_transaction.side_effect = Exception("catalog down")
    mock_dest_table.scan.return_value.count.return_value = 0  # empty dest: no truncate
    dest_pool.get.return_value = (mock_catalog, mock_dest_table)

    delivery = _make_real_delivery(state_mgr, dest_pool, ["company"], ["dest-1"], mode="full_cdc")
    with (
        patch("viaduck.main.source.snapshot_bounds", return_value=(1, 10)),
        patch("viaduck.main.source.read_cdc_changes", return_value=arrow_data),
        # Delivery threads its `_stopping` Event into `_write_with_retry`,
        # so retry backoff goes through `_backoff_sleep(delay, event)` →
        # `event.wait(delay)` — patching `time.sleep` alone would leave
        # the retry loop blocking on real Event.wait calls. Patching
        # `_backoff_sleep` covers both branches.
        patch("viaduck.apply._backoff_sleep", return_value=False),
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            dest_pool,
            router,
            cfg,
            ["dest-1"],
            {"quacksworth": "dest-1"},
            key_columns=["company"],
            mode="full_cdc",
        )
        # Keep wait_idle INSIDE the patch scope. The flush worker is
        # burning through the OCC retry budget (`_WRITE_MAX_RETRIES` attempts
        # with backoff capped at `_WRITE_MAX_DELAY_S`); if the patch has
        # already exited, the worker's next backoff is real, blocking the
        # test for minutes.
        assert delivery.wait_idle()

    state_mgr.record_error.assert_called_once()
    dest_pool.evict.assert_called()
    state_mgr.advance_cursor.assert_not_called()
    # FlushFail semantics: position reset to the persisted cursor.
    assert delivery.positions()["dest-1"] == 5


def test_poll_cycle_cdc_routing_value_mutation():
    """Cross-tenant update: preimage converted to delete for old destination;
    both destinations flush and advance."""
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    dest_pool.projection_for.return_value = None
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth"), ("dest-2", "mallardine")])
    cfg.routing.field = "company"

    state_mgr.load_cursors.return_value = {"dest-1": _cursor(5, 10), "dest-2": _cursor(5, 20)}

    # Row moves from quacksworth to mallardine
    raw_data = _cdc_table(
        [
            {"company": "quacksworth", "value": 1, "change_type": "update_preimage", "snapshot_id": 6, "rowid": 100},
            {"company": "mallardine", "value": 1, "change_type": "update_postimage", "snapshot_id": 6, "rowid": 100},
        ]
    )
    quacks_batch = _cdc_table(
        [
            {"company": "quacksworth", "value": 1, "change_type": "delete", "snapshot_id": 6, "rowid": 100},
        ]
    )
    mallard_batch = _cdc_table(
        [
            {"company": "mallardine", "value": 1, "change_type": "update_postimage", "snapshot_id": 6, "rowid": 100},
        ]
    )

    router.build_filter_expr.return_value = "company IN ('quacksworth', 'mallardine')"
    router.split_and_count.return_value = ({"quacksworth": quacks_batch, "mallardine": mallard_batch}, 0)

    mock_catalog, _txn_table = _txn_catalog()
    mock_dest_table = MagicMock()
    mock_dest_table.identifier = "dest_table"
    mock_dest_table.scan.return_value.count.return_value = 0  # empty dest: no truncate
    dest_pool.get.return_value = (mock_catalog, mock_dest_table)

    delivery = _make_real_delivery(state_mgr, dest_pool, ["company"], ["dest-1", "dest-2"], mode="full_cdc")
    with (
        patch("viaduck.main.source.snapshot_bounds", return_value=(1, 10)),
        patch("viaduck.main.source.read_cdc_changes", return_value=raw_data),
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            dest_pool,
            router,
            cfg,
            ["dest-1", "dest-2"],
            {"quacksworth": "dest-1", "mallardine": "dest-2"},
            key_columns=["company"],
            mode="full_cdc",
        )
    assert delivery.wait_idle()

    # Both destinations should have their cursors advanced
    assert state_mgr.advance_cursor.call_count == 2


def test_poll_cycle_branches_on_key_columns():
    """key_columns presence determines CDC mode: non-empty -> full_cdc, empty -> append."""
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])
    cfg.routing.field = "company"

    arrow_data = pa.table({"company": ["quacksworth"], "value": [10]})
    router.build_filter_expr.return_value = "company IN ('quacksworth')"
    router.split_and_count.return_value = ({"quacksworth": arrow_data}, 0)

    delivery = _make_delivery({"dest-1": 5})
    with (
        patch("viaduck.main.source.snapshot_bounds", return_value=(1, 10)),
        patch("viaduck.main.source.read_cdc", return_value=arrow_data) as mock_read_cdc,
        patch("viaduck.main.source.read_cdc_changes") as mock_read_changes,
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1"],
            {"quacksworth": "dest-1"},
            key_columns=[],
            mode="append_only",
        )
        mock_read_cdc.assert_called_once()
        mock_read_changes.assert_not_called()

    cdc_data = _cdc_table(
        [
            {"company": "quacksworth", "value": 10, "change_type": "insert", "snapshot_id": 6, "rowid": 1},
        ]
    )
    router.split_and_count.return_value = ({"quacksworth": cdc_data}, 0)
    delivery = _make_delivery({"dest-1": 5})
    with (
        patch("viaduck.main.source.snapshot_bounds", return_value=(1, 10)),
        patch("viaduck.main.source.read_cdc") as mock_read_cdc2,
        patch("viaduck.main.source.read_cdc_changes", return_value=cdc_data) as mock_read_changes2,
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1"],
            {"quacksworth": "dest-1"},
            key_columns=["company"],
            mode="full_cdc",
        )
        mock_read_changes2.assert_called_once()
        mock_read_cdc2.assert_not_called()


# ---------------------------------------------------------------------------
# Torture tests
# ---------------------------------------------------------------------------


def test_torture_insert_update_delete_same_key():
    """3 ops on same rowid: insert + postimage + delete -> only the
    tombstone delete survives conflict resolution."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 2, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 1, "change_type": "delete", "snapshot_id": 1, "rowid": 100},
        ]
    )
    result = _resolve_conflicts(batch)
    assert result.column("change_type").to_pylist() == ["delete"]


def test_torture_routing_value_mutation_cross_tenant():
    """Cross-tenant update: preimage becomes delete at old tenant, postimage upserts at new."""
    batch = _cdc_table(
        [
            {"company": "old_tenant", "value": 1, "change_type": "update_preimage", "snapshot_id": 1, "rowid": 100},
            {"company": "new_tenant", "value": 1, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 100},
        ]
    )
    result = _resolve_preimages(batch, "company", ["value"])
    assert result.num_rows == 2
    types = result.column("change_type").to_pylist()
    assert types[0] == "delete"
    assert types[1] == "update_postimage"
    # The delete retains the old routing value
    assert result.column("company")[0].as_py() == "old_tenant"


def test_torture_same_key_different_rows_no_cancel():
    """Different rowids with same key value should both be preserved."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 200},
        ]
    )
    result = _resolve_conflicts(batch)
    assert result.num_rows == 2


def test_torture_delete_filter_null_composite_key():
    """NULL in composite key produces IS NULL in filter."""
    rows = pa.table(
        {
            "a": pa.array([None], type=pa.int64()),
            "b": ["x"],
            "change_type": ["delete"],
            "snapshot_id": [1],
            "rowid": [10],
        }
    )
    sql = _build_delete_filter(rows, ["a", "b"])
    assert "NULL" in sql.upper()
    assert "x" in sql


def test_torture_large_composite_key():
    """5-column key, 100 rows should not crash."""
    data = {
        "k1": list(range(100)),
        "k2": [f"v{i}" for i in range(100)],
        "k3": list(range(100, 200)),
        "k4": [f"w{i}" for i in range(100)],
        "k5": list(range(200, 300)),
        "change_type": ["delete"] * 100,
        "snapshot_id": [1] * 100,
        "rowid": list(range(100)),
    }
    rows = pa.table(data)
    sql = _build_delete_filter(rows, ["k1", "k2", "k3", "k4", "k5"])
    assert "OR" in sql or "AND" in sql


def test_torture_deletes_only_changeset():
    """Batch with only deletes: no upsert, only delete filter."""
    catalog, dest_table, txn, txn_table = _mock_catalog_and_table()
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "delete", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 2, "change_type": "delete", "snapshot_id": 1, "rowid": 200},
        ]
    )
    counts = _apply_changes(catalog, dest_table, batch, ["company"])
    assert counts["deleted"] == 2
    assert counts["upserted"] == 0
    txn_table.delete.assert_called_once()
    txn_table.upsert.assert_not_called()


def test_torture_orphaned_preimage():
    """Preimage without postimage should become delete."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "update_preimage", "snapshot_id": 1, "rowid": 100},
        ]
    )
    result = _resolve_preimages(batch, "company", ["value"])
    assert result.num_rows == 1
    assert result.column("change_type")[0].as_py() == "delete"


def test_torture_empty_string_vs_null_key():
    """Empty string and None are different routing values."""
    batch = _cdc_table(
        [
            {"company": "", "value": 1, "change_type": "update_preimage", "snapshot_id": 1, "rowid": 100},
            {"company": "", "value": 2, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 100},
        ]
    )
    result = _resolve_preimages(batch, "company", ["value"])
    # Same routing value ("" == ""), preimage should be dropped
    assert result.num_rows == 1
    assert result.column("change_type")[0].as_py() == "update_postimage"


def test_torture_multiple_updates_same_key():
    """3 postimages for same key: all preserved (no conflict between postimages)."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 2, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 200},
            {"company": "acme", "value": 3, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 300},
        ]
    )
    result = _resolve_conflicts(batch)
    assert result.num_rows == 3


def test_torture_key_column_missing_from_data():
    """Missing key column raises RoutingError in preimage resolution."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
        ]
    )
    with pytest.raises(RoutingError, match="Key column 'nonexistent' not found"):
        _resolve_preimages(batch, "company", ["nonexistent"])


def test_torture_all_change_types_mixed():
    """All change types in one batch: insert, delete, update_preimage, update_postimage."""
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 2, "change_type": "delete", "snapshot_id": 1, "rowid": 200},
            {"company": "acme", "value": 3, "change_type": "update_preimage", "snapshot_id": 1, "rowid": 300},
            {"company": "acme", "value": 4, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 300},
        ]
    )
    # Preimage resolution: same tenant, drop preimage
    resolved = _resolve_preimages(batch, "company", ["value"])
    assert resolved.num_rows == 3
    types = resolved.column("change_type").to_pylist()
    assert "update_preimage" not in types
    # Conflict resolution: no conflicts (different rowids)
    final = _resolve_conflicts(resolved)
    assert final.num_rows == 3


def test_torture_special_chars_in_key_column_name():
    """Key column with @ character should work."""
    batch = pa.table(
        {
            "user@domain": ["a", "b"],
            "value": [1, 2],
            "change_type": ["insert", "insert"],
            "snapshot_id": [1, 1],
            "rowid": [100, 200],
        }
    )
    result = _resolve_preimages(batch, "user@domain", ["user@domain"])
    assert result.num_rows == 2


# ---------------------------------------------------------------------------
# Metric coverage for new CDC metrics
# ---------------------------------------------------------------------------


def test_apply_changes_upsert_matched_nonzero():
    """Verify upsert_matched reflects UpsertResult.rows_updated."""
    catalog, dest_table, txn, txn_table = _mock_catalog_and_table()
    # Simulate 1 row matched (updated), 1 row inserted
    txn_table.upsert.return_value.rows_updated = 3
    txn_table.upsert.return_value.rows_inserted = 1
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "acme", "value": 2, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 200},
            {"company": "acme", "value": 3, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 300},
            {"company": "acme", "value": 4, "change_type": "update_postimage", "snapshot_id": 1, "rowid": 400},
        ]
    )
    counts = _apply_changes(catalog, dest_table, batch, ["value"])
    assert counts["upserted"] == 4
    assert counts["upsert_matched"] == 3


def test_cdc_batch_shape_metrics_observed():
    """The raw source-read shape is observed here. The per-destination chunk
    shape is NOT — it is recorded inside delivery.buffer(), past the epoch
    guard, so discarded reads don't inflate it (see test_delivery.py)."""
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])

    arrow_data = pa.table({"company": ["quacksworth", "quacksworth", "quacksworth"], "value": [1, 2, 3]})
    router.build_filter_expr.return_value = "company IN ('quacksworth')"
    router.split_and_count.return_value = ({"quacksworth": arrow_data}, 0)

    delivery = _make_delivery({"dest-1": 5})
    with (
        patch("viaduck.main.source.snapshot_bounds", return_value=(1, 10)),
        patch("viaduck.main.source.read_cdc", return_value=arrow_data),
        patch("viaduck.main.metrics.cdc_batch_rows") as mock_batch_metric,
        patch("viaduck.main.metrics.cdc_batch_bytes") as mock_batch_bytes,
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1"],
            {"quacksworth": "dest-1"},
            key_columns=[],
            mode="append_only",
        )

    mock_batch_metric.observe.assert_called_once_with(3)
    mock_batch_bytes.observe.assert_called_once_with(arrow_data.nbytes)


# ---------------------------------------------------------------------------
# _seed_new_destinations
# ---------------------------------------------------------------------------


def test_seed_new_destinations_populates_from_scan():
    """Scan streams 3 rows in one batch; they get appended to the destination."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    cfg = _make_cfg([("dest-1", "acme")])
    cfg.routing.field = "company"
    cfg.routing.key_columns = []
    cfg.routing.seed_mode = "scan"

    state_mgr.load_cursors.return_value = {}  # no cursors -> snapshot_id=0

    rows = pa.table({"company": ["acme", "acme", "acme"], "value": [1, 2, 3]})
    mock_scan = MagicMock()
    mock_scan.to_arrow_batch_reader.return_value = iter(rows.to_batches())
    src_table.scan.return_value = mock_scan

    mock_table = MagicMock()
    mock_table.scan.return_value.count.return_value = 0  # empty dest: no truncate
    dest_pool.get.return_value = (MagicMock(), mock_table)

    with patch("viaduck.main.source.current_snapshot_id", return_value=100):
        _seed_new_destinations(src_table, state_mgr, dest_pool, cfg, ["dest-1"])

    assert mock_table.append.call_count == 1
    written = mock_table.append.call_args[0][0]
    assert written.num_rows == 3
    assert written.equals(rows)
    state_mgr.advance_cursor.assert_called_once_with("dest-1", 100, cumulative_rows=3)


def test_seed_new_destinations_skips_existing():
    """State manager returns a cursor with snapshot_id=50 (not 0). No scan happens."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    cfg = _make_cfg([("dest-1", "acme")])
    cfg.routing.field = "company"
    cfg.routing.key_columns = []
    cfg.routing.seed_mode = "scan"

    cursor = MagicMock()
    cursor.last_snapshot_id = 50
    state_mgr.load_cursors.return_value = {"dest-1": cursor}

    with patch("viaduck.main.source.current_snapshot_id", return_value=100):
        _seed_new_destinations(src_table, state_mgr, dest_pool, cfg, ["dest-1"])

    src_table.scan.assert_not_called()
    dest_pool.get.assert_not_called()


def test_seed_new_destinations_empty_source():
    """source.current_snapshot_id returns None. Nothing happens."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    cfg = _make_cfg([("dest-1", "acme")])
    cfg.routing.field = "company"
    cfg.routing.key_columns = []
    cfg.routing.seed_mode = "scan"

    with patch("viaduck.main.source.current_snapshot_id", return_value=None):
        _seed_new_destinations(src_table, state_mgr, dest_pool, cfg, ["dest-1"])

    state_mgr.load_cursors.assert_not_called()
    src_table.scan.assert_not_called()


def test_seed_new_destinations_no_matching_rows():
    """Scan returns 0 rows. No append but cursor still advanced."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    cfg = _make_cfg([("dest-1", "acme")])
    cfg.routing.field = "company"
    cfg.routing.key_columns = []
    cfg.routing.seed_mode = "scan"

    state_mgr.load_cursors.return_value = {}

    mock_scan = MagicMock()
    mock_scan.to_arrow_batch_reader.return_value = iter([])
    src_table.scan.return_value = mock_scan

    mock_table = MagicMock()
    mock_table.scan.return_value.count.return_value = 0
    dest_pool.get.return_value = (MagicMock(), mock_table)

    with patch("viaduck.main.source.current_snapshot_id", return_value=100):
        _seed_new_destinations(src_table, state_mgr, dest_pool, cfg, ["dest-1"])

    # Only the REPLACE-guard get/release; no rows means no writes.
    dest_pool.get.assert_called_once_with("dest-1")
    dest_pool.release.assert_called_once_with("dest-1")
    mock_table.upsert.assert_not_called()
    mock_table.append.assert_not_called()
    state_mgr.advance_cursor.assert_called_once_with("dest-1", 100, cumulative_rows=0)


def test_seed_new_destinations_uses_upsert_with_key_columns():
    """cfg has key_columns=['event_id']. table.upsert() called instead of append."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    cfg = _make_cfg([("dest-1", "acme")])
    cfg.routing.field = "company"
    cfg.routing.key_columns = ["event_id"]
    cfg.routing.seed_mode = "scan"

    state_mgr.load_cursors.return_value = {}

    rows = pa.table({"event_id": [1, 2], "company": ["acme", "acme"], "value": [10, 20]})
    mock_scan = MagicMock()
    mock_scan.to_arrow_batch_reader.return_value = iter(rows.to_batches())
    src_table.scan.return_value = mock_scan

    mock_table = MagicMock()
    mock_table.scan.return_value.count.return_value = 0  # empty dest: no truncate
    dest_pool.get.return_value = (MagicMock(), mock_table)

    with patch("viaduck.main.source.current_snapshot_id", return_value=100):
        _seed_new_destinations(src_table, state_mgr, dest_pool, cfg, ["dest-1"])

    assert mock_table.upsert.call_count == 1
    written = mock_table.upsert.call_args[0][0]
    assert written.equals(rows)
    assert mock_table.upsert.call_args[1] == {"join_cols": ["event_id"]}
    mock_table.append.assert_not_called()


def test_seed_new_destinations_uses_append_without_key_columns():
    """cfg has key_columns=[]. table.append() called."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    cfg = _make_cfg([("dest-1", "acme")])
    cfg.routing.field = "company"
    cfg.routing.key_columns = []
    cfg.routing.seed_mode = "scan"

    state_mgr.load_cursors.return_value = {}

    rows = pa.table({"company": ["acme", "acme"], "value": [10, 20]})
    mock_scan = MagicMock()
    mock_scan.to_arrow_batch_reader.return_value = iter(rows.to_batches())
    src_table.scan.return_value = mock_scan

    mock_table = MagicMock()
    mock_table.scan.return_value.count.return_value = 0  # empty dest: no truncate
    dest_pool.get.return_value = (MagicMock(), mock_table)

    with patch("viaduck.main.source.current_snapshot_id", return_value=100):
        _seed_new_destinations(src_table, state_mgr, dest_pool, cfg, ["dest-1"])

    assert mock_table.append.call_count == 1
    written = mock_table.append.call_args[0][0]
    assert written.equals(rows)
    mock_table.upsert.assert_not_called()


def test_seed_new_destinations_multiple():
    """Multiple new destinations seeded independently."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    cfg = _make_cfg([("dest-1", "acme"), ("dest-2", "beta")])
    cfg.routing.field = "company"
    cfg.routing.key_columns = []
    cfg.routing.seed_mode = "scan"

    state_mgr.load_cursors.return_value = {}  # both at snapshot_id=0

    rows_acme = pa.table({"company": ["acme"], "value": [1]})
    rows_beta = pa.table({"company": ["beta"], "value": [2]})

    def mock_scan(row_filter, snapshot_id=None):
        scan = MagicMock()
        # EqualTo stores the value — extract it from the filter
        if "acme" in str(row_filter):
            scan.to_arrow_batch_reader.return_value = iter(rows_acme.to_batches())
        else:
            scan.to_arrow_batch_reader.return_value = iter(rows_beta.to_batches())
        return scan

    src_table.scan.side_effect = mock_scan

    tables = {}

    def mock_get(dest_id):
        t = tables.get(dest_id) or MagicMock()
        t.scan.return_value.count.return_value = 0
        tables[dest_id] = t
        return (MagicMock(), t)

    dest_pool.get.side_effect = mock_get

    with patch("viaduck.main.source.current_snapshot_id", return_value=100):
        _seed_new_destinations(src_table, state_mgr, dest_pool, cfg, ["dest-1", "dest-2"])

    assert state_mgr.advance_cursor.call_count == 2
    assert "dest-1" in tables
    assert "dest-2" in tables


def test_seed_new_destinations_pins_snapshot():
    """Scan should be pinned to the captured snapshot_id."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    cfg = _make_cfg([("dest-1", "acme")])
    cfg.routing.field = "company"
    cfg.routing.key_columns = []
    cfg.routing.seed_mode = "scan"

    state_mgr.load_cursors.return_value = {}

    mock_scan = MagicMock()
    mock_scan.to_arrow_batch_reader.return_value = iter([])
    src_table.scan.return_value = mock_scan

    mock_table = MagicMock()
    mock_table.scan.return_value.count.return_value = 0
    dest_pool.get.return_value = (MagicMock(), mock_table)

    with patch("viaduck.main.source.current_snapshot_id", return_value=42):
        _seed_new_destinations(src_table, state_mgr, dest_pool, cfg, ["dest-1"])

    # Verify scan was called with snapshot_id pinned to the captured value
    call_kwargs = src_table.scan.call_args.kwargs
    assert call_kwargs["snapshot_id"] == 42


# ---------------------------------------------------------------------------
# _start_progress_heartbeat
# ---------------------------------------------------------------------------
#
# These tests run the tick loop synchronously: the spawned thread is replaced
# with a no-op, `time.monotonic` is driven by a virtual clock dict, and
# `stop.wait` advances the clock by its `timeout` argument and decides when
# to terminate the loop. No real sleeps, no real threads — fully deterministic.


def _install_fake_heartbeat_runtime(monkeypatch):
    """Patch threading.Thread + time.monotonic. Returns (captured, clock).

    `captured["target"]` is the tick callable, runnable synchronously.
    `clock["t"]` is the virtual clock; advance it from the fake `wait`.
    """
    captured: dict = {}

    class _FakeThread:
        def __init__(self, *, target, daemon):
            captured["target"] = target

        def start(self):
            pass

    monkeypatch.setattr("viaduck.main.threading.Thread", _FakeThread)

    clock = {"t": 0.0}
    monkeypatch.setattr("viaduck.main.time.monotonic", lambda: clock["t"])
    return captured, clock


def test_progress_heartbeat_logs_state_with_rate(caplog, monkeypatch):
    """With a state dict, the heartbeat logs rows + batches + derived rate."""
    captured, clock = _install_fake_heartbeat_runtime(monkeypatch)

    state = {"rows": 10, "batches": 4}
    stop = _start_progress_heartbeat("test-label", interval_s=2.0, state=state)

    waits: list[float] = []

    def fake_wait(timeout):
        waits.append(timeout)
        clock["t"] += timeout
        return len(waits) >= 2  # one tick logs, then exit

    monkeypatch.setattr(stop, "wait", fake_wait)

    with caplog.at_level("INFO", logger="viaduck.main"):
        captured["target"]()

    msgs = [r.message for r in caplog.records if "test-label" in r.message]
    assert msgs, f"No heartbeat log captured; records: {[r.message for r in caplog.records]}"
    msg = msgs[0]
    assert "10 rows" in msg
    assert "4 batches" in msg
    # rate = 10 rows / 2.0s elapsed = 5 rows/s
    assert "5 rows/s" in msg


def test_progress_heartbeat_falls_back_to_still_working_without_state(caplog, monkeypatch):
    """Without a state arg, the heartbeat keeps the still-working format."""
    captured, clock = _install_fake_heartbeat_runtime(monkeypatch)

    stop = _start_progress_heartbeat("plain-label", interval_s=2.0)

    waits: list[float] = []

    def fake_wait(timeout):
        waits.append(timeout)
        clock["t"] += timeout
        return len(waits) >= 2

    monkeypatch.setattr(stop, "wait", fake_wait)

    with caplog.at_level("INFO", logger="viaduck.main"):
        captured["target"]()

    msgs = [r.message for r in caplog.records if "plain-label" in r.message]
    assert msgs
    assert "still working" in msgs[0]


def test_progress_heartbeat_state_updates_visible_to_thread(caplog, monkeypatch):
    """A mid-loop mutation by the writer is visible to the next reader log."""
    captured, clock = _install_fake_heartbeat_runtime(monkeypatch)

    state = {"rows": 0, "batches": 0}
    stop = _start_progress_heartbeat("growing", interval_s=1.0, state=state)

    call_count = {"n": 0}

    def fake_wait(timeout):
        call_count["n"] += 1
        clock["t"] += timeout
        if call_count["n"] == 1:
            state["rows"] = 100
            state["batches"] = 10
        return call_count["n"] >= 2

    monkeypatch.setattr(stop, "wait", fake_wait)

    with caplog.at_level("INFO", logger="viaduck.main"):
        captured["target"]()

    msgs = [r.message for r in caplog.records if "growing" in r.message]
    assert any("100 rows" in m and "10 batches" in m for m in msgs)


def test_progress_heartbeat_uses_pre_progress_label_when_rows_zero(caplog, monkeypatch):
    """With state present but `rows == 0`, the tick uses `pre_progress_label`, not the rate format."""
    captured, clock = _install_fake_heartbeat_runtime(monkeypatch)

    state = {"rows": 0, "batches": 0}
    stop = _start_progress_heartbeat(
        "seed-label",
        interval_s=2.0,
        state=state,
        pre_progress_label="DuckDB pre-execution",
    )

    waits: list[float] = []

    def fake_wait(timeout):
        waits.append(timeout)
        clock["t"] += timeout
        return len(waits) >= 2

    monkeypatch.setattr(stop, "wait", fake_wait)

    with caplog.at_level("INFO", logger="viaduck.main"):
        captured["target"]()

    msgs = [r.message for r in caplog.records if "seed-label" in r.message]
    assert msgs
    msg = msgs[0]
    assert "DuckDB pre-execution" in msg
    assert "rows/s" not in msg
    assert "0 rows in 0 batches" not in msg


def test_progress_heartbeat_pre_progress_default_label(caplog, monkeypatch):
    """Without an explicit `pre_progress_label`, the default `no progress yet` is used."""
    captured, clock = _install_fake_heartbeat_runtime(monkeypatch)

    state = {"rows": 0, "batches": 0}
    stop = _start_progress_heartbeat("default-label", interval_s=2.0, state=state)

    waits: list[float] = []

    def fake_wait(timeout):
        waits.append(timeout)
        clock["t"] += timeout
        return len(waits) >= 2

    monkeypatch.setattr(stop, "wait", fake_wait)

    with caplog.at_level("INFO", logger="viaduck.main"):
        captured["target"]()

    msgs = [r.message for r in caplog.records if "default-label" in r.message]
    assert msgs
    assert "no progress yet" in msgs[0]


def test_progress_heartbeat_transitions_pre_progress_to_rate(caplog, monkeypatch):
    """First tick logs pre-progress; once `rows` is non-zero, subsequent ticks log the rate format."""
    captured, clock = _install_fake_heartbeat_runtime(monkeypatch)

    state = {"rows": 0, "batches": 0}
    stop = _start_progress_heartbeat(
        "transition-label",
        interval_s=2.0,
        state=state,
        pre_progress_label="DuckDB pre-execution",
    )

    call_count = {"n": 0}

    def fake_wait(timeout):
        call_count["n"] += 1
        clock["t"] += timeout
        # Leave rows at 0 through the first log; mutate before the second wait
        # so the second tick reads a positive rows count.
        if call_count["n"] == 2:
            state["rows"] = 50
            state["batches"] = 5
        return call_count["n"] >= 3

    monkeypatch.setattr(stop, "wait", fake_wait)

    with caplog.at_level("INFO", logger="viaduck.main"):
        captured["target"]()

    msgs = [r.message for r in caplog.records if "transition-label" in r.message]
    assert len(msgs) >= 2, f"Expected at least 2 ticks; got: {msgs}"
    assert "DuckDB pre-execution" in msgs[0]
    assert "rows/s" not in msgs[0]
    assert "50 rows" in msgs[1]
    assert "5 batches" in msgs[1]
    assert "rows/s" in msgs[1]


def test_progress_heartbeat_early_to_normal_cadence_transition(monkeypatch):
    """Heartbeat fires every `early_interval_s` until elapsed >= early_duration_s, then `interval_s`."""
    captured, clock = _install_fake_heartbeat_runtime(monkeypatch)

    stop = _start_progress_heartbeat(
        "transition-label",
        interval_s=30.0,
        early_interval_s=5.0,
        early_duration_s=60.0,
    )

    waits: list[float] = []

    def fake_wait(timeout):
        waits.append(timeout)
        clock["t"] += timeout
        # Stop once we have at least one early + one normal tick.
        return any(w == 30.0 for w in waits)

    monkeypatch.setattr(stop, "wait", fake_wait)

    captured["target"]()

    early = [w for w in waits if w == 5.0]
    normal = [w for w in waits if w == 30.0]
    assert early, f"Expected early-window waits of 5.0s; got: {waits}"
    assert normal, f"Expected at least one normal-cadence wait of 30.0s; got: {waits}"
    # All early waits precede all normal waits.
    last_early_idx = max(i for i, w in enumerate(waits) if w == 5.0)
    first_normal_idx = min(i for i, w in enumerate(waits) if w == 30.0)
    assert last_early_idx < first_normal_idx, f"Early/normal waits interleaved: {waits}"


# ---------------------------------------------------------------------------
# _seed_new_destinations: pre-scan stats + first-batch milestone
# ---------------------------------------------------------------------------


def test_seed_new_destinations_logs_prescan_stats(caplog):
    """`inspect().files(...)` is summed and a one-line stats summary is logged."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    cfg = _make_cfg([("dest-1", "acme")])
    cfg.routing.field = "company"
    cfg.routing.key_columns = []
    cfg.routing.seed_mode = "scan"

    state_mgr.load_cursors.return_value = {}

    files_table = pa.table(
        {
            "data_file_size_bytes": [1024**3, 2 * 1024**3],  # 1 GiB + 2 GiB = 3 GiB
            "delete_file_size_bytes": [512 * 1024, 256 * 1024],  # 768 KiB = 0.75 MiB
        }
    )
    src_table.inspect.return_value.files.return_value = files_table

    rows = pa.table({"company": ["acme"], "value": [1]})
    mock_scan = MagicMock()
    mock_scan.to_arrow_batch_reader.return_value = iter(rows.to_batches())
    src_table.scan.return_value = mock_scan

    mock_table = MagicMock()
    mock_table.scan.return_value.count.return_value = 0  # empty dest: no truncate
    dest_pool.get.return_value = (MagicMock(), mock_table)

    with patch("viaduck.main.source.current_snapshot_id", return_value=42):
        with caplog.at_level("INFO", logger="viaduck.main"):
            _seed_new_destinations(src_table, state_mgr, dest_pool, cfg, ["dest-1"])

    src_table.inspect.return_value.files.assert_called_once_with(snapshot_id=42)

    msgs = [r.message for r in caplog.records]
    prescan = [m for m in msgs if "Source snapshot 42" in m and "data files" in m]
    assert prescan, f"No prescan stats line; got: {msgs}"
    line = prescan[0]
    assert "2 data files" in line
    assert "3.00 GiB" in line
    assert "768.00 KiB" in line


def test_seed_new_destinations_continues_when_prescan_stats_fail(caplog):
    """If `inspect().files` raises, seed completes anyway and the failure is logged."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    cfg = _make_cfg([("dest-1", "acme")])
    cfg.routing.field = "company"
    cfg.routing.key_columns = []
    cfg.routing.seed_mode = "scan"

    state_mgr.load_cursors.return_value = {}
    src_table.inspect.return_value.files.side_effect = RuntimeError("metadata unavailable")

    rows = pa.table({"company": ["acme"], "value": [1]})
    mock_scan = MagicMock()
    mock_scan.to_arrow_batch_reader.return_value = iter(rows.to_batches())
    src_table.scan.return_value = mock_scan

    mock_table = MagicMock()
    mock_table.scan.return_value.count.return_value = 0  # empty dest: no truncate
    dest_pool.get.return_value = (MagicMock(), mock_table)

    with patch("viaduck.main.source.current_snapshot_id", return_value=42):
        with caplog.at_level("WARNING", logger="viaduck.main"):
            _seed_new_destinations(src_table, state_mgr, dest_pool, cfg, ["dest-1"])

    state_mgr.advance_cursor.assert_called_once()
    err = [r for r in caplog.records if r.levelname == "ERROR" and "snapshot file inventory" in r.message]
    assert err, f"Expected ERROR log for failed prescan; got: {[(r.levelname, r.message) for r in caplog.records]}"


def test_seed_new_destinations_logs_first_batch_milestone(caplog):
    """The first batch from `to_arrow_batch_reader` triggers a milestone log line."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    cfg = _make_cfg([("dest-1", "acme")])
    cfg.routing.field = "company"
    cfg.routing.key_columns = []
    cfg.routing.seed_mode = "scan"

    state_mgr.load_cursors.return_value = {}

    rows = pa.table({"company": ["acme", "acme"], "value": [1, 2]})
    mock_scan = MagicMock()
    mock_scan.to_arrow_batch_reader.return_value = iter(rows.to_batches())
    src_table.scan.return_value = mock_scan

    mock_table = MagicMock()
    mock_table.scan.return_value.count.return_value = 0  # empty dest: no truncate
    dest_pool.get.return_value = (MagicMock(), mock_table)

    with patch("viaduck.main.source.current_snapshot_id", return_value=99):
        with caplog.at_level("INFO", logger="viaduck.main"):
            _seed_new_destinations(src_table, state_mgr, dest_pool, cfg, ["dest-1"])

    msgs = [r.message for r in caplog.records]
    milestone = [m for m in msgs if "first batch in" in m and "dest-1" in m]
    assert milestone, f"No first-batch milestone line; got: {msgs}"
    assert "DuckDB pre-execution complete" in milestone[0]
    assert "streaming started" in milestone[0]


# ---------------------------------------------------------------------------
# Winner(k): per-key last-write-wins on upsert candidates (spec Phase3Apply)
# ---------------------------------------------------------------------------


def test_apply_changes_dedupes_upsert_candidates_per_key():
    """Multiple upsert candidates for one key (an insert + later postimages
    across a buffered window) collapse to the highest-snapshot candidate
    BEFORE the upsert — the spec's Winner(k). Found by the M3 soak:
    without this, pyducklake upsert receives duplicate join keys and the
    destination grows duplicate rows."""
    catalog, dest_table, txn, txn_table = _mock_catalog_and_table()
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 3, "rowid": 100},
            {"company": "acme", "value": 2, "change_type": "update_postimage", "snapshot_id": 5, "rowid": 100},
            {"company": "acme", "value": 3, "change_type": "update_postimage", "snapshot_id": 9, "rowid": 100},
            {"company": "other", "value": 7, "change_type": "insert", "snapshot_id": 4, "rowid": 200},
        ]
    )
    counts = _apply_changes(catalog, dest_table, batch, ["company"])
    assert counts["upserted"] == 2  # one winner per key
    sent = txn_table.upsert.call_args[0][0]
    by_company = dict(zip(sent.column("company").to_pylist(), sent.column("value").to_pylist()))
    assert by_company == {"acme": 3, "other": 7}  # highest snapshot wins


def test_apply_changes_winner_tiebreak_by_rowid():
    """Same snapshot for two candidates of one key (out-of-contract but
    possible in a replayed union): rowid breaks the tie deterministically."""
    catalog, dest_table, txn, txn_table = _mock_catalog_and_table()
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "insert", "snapshot_id": 5, "rowid": 100},
            {"company": "acme", "value": 2, "change_type": "insert", "snapshot_id": 5, "rowid": 300},
        ]
    )
    _apply_changes(catalog, dest_table, batch, ["company"])
    sent = txn_table.upsert.call_args[0][0]
    assert sent.column("value").to_pylist() == [2]  # rowid 300 wins


def test_apply_changes_winner_preserves_null_keys():
    """NULL key values must survive Winner(k): Acero hash joins silently
    drop null keys, which is why winner selection is take()-based. Null
    keys form their own group and dedupe last-write-wins like any other."""
    catalog, dest_table, txn, txn_table = _mock_catalog_and_table()
    batch = _cdc_table(
        [
            {"company": None, "value": 1, "change_type": "insert", "snapshot_id": 3, "rowid": 100},
            {"company": None, "value": 2, "change_type": "update_postimage", "snapshot_id": 6, "rowid": 100},
            {"company": "acme", "value": 7, "change_type": "insert", "snapshot_id": 4, "rowid": 200},
        ]
    )
    counts = _apply_changes(catalog, dest_table, batch, ["company"])
    assert counts["upserted"] == 2
    sent = txn_table.upsert.call_args[0][0]
    by_company = dict(zip(sent.column("company").to_pylist(), sent.column("value").to_pylist()))
    assert by_company == {None: 2, "acme": 7}


def test_apply_changes_winner_composite_key_null_component():
    """A NULL in one component of a composite key must also survive."""
    catalog, dest_table, txn, txn_table = _mock_catalog_and_table()
    batch = _cdc_table(
        [
            {"company": "acme", "region": None, "value": 1, "change_type": "insert", "snapshot_id": 3, "rowid": 100},
            {
                "company": "acme",
                "region": None,
                "value": 2,
                "change_type": "update_postimage",
                "snapshot_id": 5,
                "rowid": 100,
            },
            {"company": "acme", "region": "us", "value": 9, "change_type": "insert", "snapshot_id": 4, "rowid": 200},
        ]
    )
    counts = _apply_changes(catalog, dest_table, batch, ["company", "region"])
    assert counts["upserted"] == 2
    sent = txn_table.upsert.call_args[0][0]
    got = set(zip(sent.column("region").to_pylist(), sent.column("value").to_pylist()))
    assert got == {(None, 2), ("us", 9)}


def test_apply_changes_key_reuse_delete_before_upsert():
    """Key takeover within one window: delete of rowid r1 (key k) plus a
    surviving insert of rowid r2 (same k). Phase 2 keys on rowid so both
    survive to Phase 3, which must apply the delete BEFORE the upsert or
    the reinserted row is eaten (spec afterDelete/afterUpsert order)."""
    catalog, dest_table, txn, txn_table = _mock_catalog_and_table()
    order = MagicMock()
    order.attach_mock(txn_table.delete, "delete")
    order.attach_mock(txn_table.upsert, "upsert")
    batch = _cdc_table(
        [
            {"company": "acme", "value": 1, "change_type": "delete", "snapshot_id": 4, "rowid": 100},
            {"company": "acme", "value": 5, "change_type": "insert", "snapshot_id": 6, "rowid": 300},
        ]
    )
    counts = _apply_changes(catalog, dest_table, batch, ["company"])
    assert counts["deleted"] == 1
    assert counts["upserted"] == 1
    calls = [name for name, _, _ in order.mock_calls if name in ("delete", "upsert")]
    assert calls == ["delete", "upsert"]
    sent = txn_table.upsert.call_args[0][0]
    assert sent.column("value").to_pylist() == [5]


# ---------------------------------------------------------------------------
# Destination status derivation (web UI / status API)
# ---------------------------------------------------------------------------


def _delivery_status(**kw):
    from viaduck.delivery import DestDeliveryStatus

    base = dict(
        flushed_snapshot=10,
        position_snapshot=10,
        rows_replicated=0,
        last_error=None,
        buffer_rows=0,
        buffer_age_s=0.0,
        flushing=False,
    )
    base.update(kw)
    return DestDeliveryStatus(**base)


@pytest.mark.parametrize(
    ("snap_now", "kw", "expected"),
    [
        # error beats everything
        (12, {"last_error": "boom", "buffer_rows": 5}, "error"),
        (10, {"flushing": True}, "flushing"),
        # reads behind the source: genuinely lagging
        (12, {"position_snapshot": 10}, "lagging"),
        # read-current, data awaiting flush: the buffering design working
        (12, {"position_snapshot": 12, "buffer_rows": 5}, "buffering"),
        # read-current, empty buffer but unpersisted position advance
        (12, {"position_snapshot": 12, "flushed_snapshot": 10}, "buffering"),
        (10, {}, "healthy"),
    ],
)
def test_derive_dest_status(snap_now, kw, expected):
    """Between flushes the cursor always trails the source; that must NOT
    display as 'lagging' — only reads being behind is operationally lag."""
    assert _derive_dest_status(_delivery_status(**kw), snap_now) == expected


def test_phase1_converted_delete_survives_phase2_as_tombstone():
    """Phase 1 converts a cross-tenant preimage to a delete; when the same
    rowid's insert is in the window (row created then migrated), the OLD
    tenant's routed batch carries insert+converted-delete for one rowid —
    Phase 2 must keep the converted delete (tombstone) so the old copy is
    removed. Mirrors the real pipeline order: Phase 1 → route → Phase 2
    per destination."""
    import pyarrow.compute as pc

    batch = _cdc_table(
        [
            {"company": "old_tenant", "value": 1, "change_type": "insert", "snapshot_id": 1, "rowid": 100},
            {"company": "old_tenant", "value": 1, "change_type": "update_preimage", "snapshot_id": 2, "rowid": 100},
            {"company": "new_tenant", "value": 1, "change_type": "update_postimage", "snapshot_id": 2, "rowid": 100},
        ]
    )
    resolved = _resolve_preimages(batch, "company", ["value"])

    # Route by company (what the Router does), then Phase 2 per destination.
    old_routed = resolved.filter(pc.equal(resolved.column("company"), "old_tenant"))
    new_routed = resolved.filter(pc.equal(resolved.column("company"), "new_tenant"))

    old_resolved = _resolve_conflicts(old_routed)
    assert old_resolved.column("change_type").to_pylist() == ["delete"]  # tombstone survives

    new_resolved = _resolve_conflicts(new_routed)
    assert new_resolved.column("change_type").to_pylist() == ["update_postimage"]


# --- seed-scan progress reporting ---


def test_fmt_duration():
    assert _fmt_duration(42) == "42s"
    assert _fmt_duration(90) == "2m"  # rounds
    assert _fmt_duration(25 * 60) == "25m"
    assert _fmt_duration(3 * 3600 + 17 * 60) == "3h 17m"


def test_scan_progress_suffix_no_conn():
    assert _scan_progress_suffix(None, 100.0) == ""


def test_scan_progress_suffix_no_query_running():
    conn = MagicMock()
    conn.query_progress.return_value = -1.0
    assert _scan_progress_suffix(conn, 100.0) == ""


def test_scan_progress_suffix_sub_one_percent_suppressed():
    """Fractional early readings must not render '~0%' with a noise ETA."""
    conn = MagicMock()
    for pct in (0.0, 0.3, 0.99):
        conn.query_progress.return_value = pct
        assert _scan_progress_suffix(conn, 100.0) == ""


def test_scan_progress_suffix_midway():
    conn = MagicMock()
    conn.query_progress.return_value = 25.0
    # 25% in 100s -> 300s remaining -> 5m
    assert _scan_progress_suffix(conn, 100.0) == " (~25% scanned, est. 5m remaining)"


def test_scan_progress_suffix_complete():
    conn = MagicMock()
    conn.query_progress.return_value = 100.0
    assert _scan_progress_suffix(conn, 100.0) == " (~100% scanned)"


def test_scan_progress_suffix_swallows_errors_warning_once(caplog, monkeypatch):
    monkeypatch.setattr("viaduck.main._scan_progress_poll_warned", False)
    conn = MagicMock()
    conn.query_progress.side_effect = RuntimeError("connection busy")
    with caplog.at_level("WARNING", logger="viaduck.main"):
        assert _scan_progress_suffix(conn, 100.0) == ""
        assert _scan_progress_suffix(conn, 200.0) == ""
    warnings = [r for r in caplog.records if "progress polling failed" in r.message]
    assert len(warnings) == 1


def test_scan_progress_suffix_recovers_after_transient_error(monkeypatch):
    """A failed poll (e.g. closed-connection race) must not disable polling."""
    monkeypatch.setattr("viaduck.main._scan_progress_poll_warned", False)
    conn = MagicMock()
    conn.query_progress.side_effect = [RuntimeError("teardown race"), 50.0]
    assert _scan_progress_suffix(conn, 100.0) == ""
    assert "50%" in _scan_progress_suffix(conn, 100.0)


def test_progress_heartbeat_suffix_in_rate_line(caplog, monkeypatch):
    """progress_conn suffix lands in the rows-flowing log variant."""
    captured, clock = _install_fake_heartbeat_runtime(monkeypatch)
    conn = MagicMock()
    conn.query_progress.return_value = 25.0

    state = {"rows": 10, "batches": 4}
    stop = _start_progress_heartbeat("rate-label", interval_s=100.0, state=state, progress_conn=conn)

    waits: list[float] = []

    def fake_wait(timeout):
        waits.append(timeout)
        clock["t"] += timeout
        return len(waits) >= 2

    monkeypatch.setattr(stop, "wait", fake_wait)
    with caplog.at_level("INFO", logger="viaduck.main"):
        captured["target"]()

    msgs = [r.message for r in caplog.records if "rate-label" in r.message]
    assert msgs
    # 25% in 100s -> 300s -> 5m remaining
    assert "10 rows" in msgs[0]
    assert "(~25% scanned, est. 5m remaining)" in msgs[0]


def test_progress_heartbeat_suffix_in_pre_progress_line(caplog, monkeypatch):
    """progress_conn suffix lands in the pre-execution variant — the original blind spot."""
    captured, clock = _install_fake_heartbeat_runtime(monkeypatch)
    conn = MagicMock()
    conn.query_progress.return_value = 10.0

    state = {"rows": 0, "batches": 0}
    stop = _start_progress_heartbeat(
        "pre-label",
        interval_s=100.0,
        state=state,
        pre_progress_label="DuckDB pre-execution",
        progress_conn=conn,
    )

    waits: list[float] = []

    def fake_wait(timeout):
        waits.append(timeout)
        clock["t"] += timeout
        return len(waits) >= 2

    monkeypatch.setattr(stop, "wait", fake_wait)
    with caplog.at_level("INFO", logger="viaduck.main"):
        captured["target"]()

    msgs = [r.message for r in caplog.records if "pre-label" in r.message]
    assert msgs
    assert "DuckDB pre-execution" in msgs[0]
    # 10% in 100s -> 900s -> 15m remaining
    assert "(~10% scanned, est. 15m remaining)" in msgs[0]


def test_progress_heartbeat_suffix_in_still_working_line(caplog, monkeypatch):
    """progress_conn suffix lands in the no-state variant."""
    captured, clock = _install_fake_heartbeat_runtime(monkeypatch)
    conn = MagicMock()
    conn.query_progress.return_value = 50.0

    stop = _start_progress_heartbeat("plain-progress", interval_s=100.0, progress_conn=conn)

    waits: list[float] = []

    def fake_wait(timeout):
        waits.append(timeout)
        clock["t"] += timeout
        return len(waits) >= 2

    monkeypatch.setattr(stop, "wait", fake_wait)
    with caplog.at_level("INFO", logger="viaduck.main"):
        captured["target"]()

    msgs = [r.message for r in caplog.records if "plain-progress" in r.message]
    assert msgs
    assert "still working" in msgs[0]
    assert "(~50% scanned" in msgs[0]


def test_seed_passes_source_connection_to_heartbeat():
    """The seed path must wire src_table.catalog.connection into the heartbeat;
    a regression to progress_conn=None passes every other test silently."""
    src_table = MagicMock()
    state_mgr = MagicMock()
    dest_pool = MagicMock()
    cfg = _make_cfg([("dest-1", "acme")])
    cfg.routing.field = "company"
    cfg.routing.key_columns = []
    cfg.routing.seed_mode = "scan"

    state_mgr.load_cursors.return_value = {}

    rows = pa.table({"company": ["acme"], "value": [1]})
    mock_scan = MagicMock()
    mock_scan.to_arrow_batch_reader.return_value = iter(rows.to_batches())
    src_table.scan.return_value = mock_scan

    mock_table = MagicMock()
    mock_table.scan.return_value.count.return_value = 0
    dest_pool.get.return_value = (MagicMock(), mock_table)

    with patch("viaduck.main.source.current_snapshot_id", return_value=42):
        with patch("viaduck.main._start_progress_heartbeat") as mock_hb:
            _seed_new_destinations(src_table, state_mgr, dest_pool, cfg, ["dest-1"])

    seed_calls = [c for c in mock_hb.call_args_list if "Seed scan" in c.args[0]]
    assert seed_calls, f"No seed-scan heartbeat started; calls: {mock_hb.call_args_list}"
    assert seed_calls[0].kwargs["progress_conn"] is src_table.catalog.connection


# ---------------------------------------------------------------------------
# _initial_snapshot_id: seed_mode earliest / latest / scan
# ---------------------------------------------------------------------------


def test_initial_snapshot_id_latest_returns_current_head():
    """seed_mode=latest returns the current head snapshot_id."""
    src_table = MagicMock()
    with patch("viaduck.main.source.current_snapshot_id", return_value=9_999_999):
        assert _initial_snapshot_id("latest", src_table) == 9_999_999


def test_initial_snapshot_id_latest_falls_back_to_zero_on_empty_catalog():
    """seed_mode=latest returns 0 when no snapshots exist."""
    src_table = MagicMock()
    with patch("viaduck.main.source.current_snapshot_id", return_value=None):
        assert _initial_snapshot_id("latest", src_table) == 0


def test_initial_snapshot_id_earliest_returns_min_minus_one():
    """seed_mode=earliest returns MIN(snapshot_id) - 1 so CDC range includes the first snapshot."""
    src_table = MagicMock()
    with patch("viaduck.main.source.earliest_snapshot_id", return_value=5_000_000):
        assert _initial_snapshot_id("earliest", src_table) == 4_999_999


def test_initial_snapshot_id_earliest_falls_back_to_zero_on_empty_catalog():
    """seed_mode=earliest returns 0 when no snapshots exist."""
    src_table = MagicMock()
    with patch("viaduck.main.source.earliest_snapshot_id", return_value=None):
        assert _initial_snapshot_id("earliest", src_table) == 0


def test_initial_snapshot_id_scan_returns_zero():
    """scan mode always returns 0; _seed_new_destinations advances from there."""
    assert _initial_snapshot_id("scan", MagicMock()) == 0


# ---------------------------------------------------------------------------
# Log throttles: buffer-watermark WARN + [MEMTRACE] INFO
# ---------------------------------------------------------------------------


def test_watermark_paused_logs_once_then_heartbeats(caplog):
    """First entry logs immediately. Subsequent calls inside the heartbeat
    interval are suppressed. After the interval, a "still exceeded" WARN
    fires with the accumulated stall duration."""
    import viaduck.main as vmain

    # Reset stall state so this test is independent of any earlier tests.
    vmain._watermark_stall_start = 0.0
    vmain._last_watermark_warn_at = 0.0

    fake_now = [1000.0]

    def fake_monotonic():
        return fake_now[0]

    with caplog.at_level("WARNING", logger="viaduck.main"):
        with patch("viaduck.main.time.monotonic", side_effect=fake_monotonic):
            vmain._log_watermark_paused("all flushes in flight")
            # Immediate re-entry within the heartbeat interval — no new log.
            fake_now[0] += 5.0
            vmain._log_watermark_paused("all flushes in flight")
            # Past the heartbeat interval — one "still exceeded" line fires.
            fake_now[0] += vmain._WATERMARK_HEARTBEAT_INTERVAL_S + 1.0
            vmain._log_watermark_paused("all flushes in flight")

    warns = [r for r in caplog.records if "Buffer watermark" in r.getMessage()]
    assert len(warns) == 2, f"expected 2 warns (entry + heartbeat), got {len(warns)}"
    assert "pausing CDC reads" in warns[0].getMessage()
    assert "still exceeded" in warns[1].getMessage()
    assert f"{int(vmain._WATERMARK_HEARTBEAT_INTERVAL_S + 6)}s" in warns[1].getMessage()


def test_watermark_cleared_emits_edge_only_when_previously_stalled(caplog):
    """`_log_watermark_cleared` is a no-op when no stall was active. When one
    was active, it logs an INFO edge with the stall duration and resets."""
    import viaduck.main as vmain

    # Case 1: no active stall — cleared() is a no-op.
    vmain._watermark_stall_start = 0.0
    with caplog.at_level("INFO", logger="viaduck.main"):
        vmain._log_watermark_cleared()
    infos = [r for r in caplog.records if "watermark" in r.getMessage().lower()]
    assert infos == [], "no log expected when there was no active stall"

    # Case 2: an active stall — cleared() logs and resets.
    caplog.clear()
    vmain._watermark_stall_start = 500.0
    vmain._last_watermark_warn_at = 500.0
    with caplog.at_level("INFO", logger="viaduck.main"):
        with patch("viaduck.main.time.monotonic", return_value=560.0):
            vmain._log_watermark_cleared()
    infos = [r for r in caplog.records if "cleared" in r.getMessage()]
    assert len(infos) == 1
    assert "60s" in infos[0].getMessage()
    assert vmain._watermark_stall_start == 0.0


def test_memtrace_throttle_short_circuits_query(caplog):
    """Throttled calls must skip the /proc + duckdb_memory() work entirely —
    the whole point is to suppress the log AND avoid the per-cycle cost."""
    import viaduck.main as vmain

    vmain._last_memtrace_at = 0.0

    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchall.return_value = []

    def fake_open(*args, **kwargs):
        return iter(["VmRSS:  1024 kB\n", "VmSize: 2048 kB\n"])

    fake_now = [1000.0]

    with (
        patch("viaduck.main.time.monotonic", side_effect=lambda: fake_now[0]),
        patch("builtins.open", side_effect=fake_open),
    ):
        vmain._log_memory_stats(mock_conn)  # fires
        fake_now[0] += 2.0
        vmain._log_memory_stats(mock_conn)  # throttled
        fake_now[0] += vmain._MEMTRACE_MIN_INTERVAL_S + 1.0
        vmain._log_memory_stats(mock_conn)  # fires

    # duckdb_memory() query only issued on the two firing calls.
    assert mock_conn.execute.call_count == 2, (
        f"throttle failed: query ran {mock_conn.execute.call_count} times, expected 2"
    )


# ---------------------------------------------------------------------------
# _export_dest_time_lag (exact wall-clock lag gauge)
# ---------------------------------------------------------------------------


def test_export_dest_time_lag_sets_exact_age():
    from datetime import datetime, timedelta

    from viaduck.main import _export_dest_time_lag

    metrics.init("test")  # re-bind: earlier test modules re-init with other pipelines
    delivery = _make_delivery({"dest-1": 10})
    snapshot = delivery.status_snapshot.return_value
    ts = datetime.now(UTC) - timedelta(seconds=120)
    with patch("viaduck.main.source.snapshot_times", return_value={10: ts}) as st:
        _export_dest_time_lag(MagicMock(), snapshot, ["dest-1"], snap_now=100)
    st.assert_called_once()
    assert st.call_args[0][1] == [10]
    val = metrics._dest_time_lag_seconds.labels(pipeline="test", destination="dest-1")._value.get()
    assert 119 <= val <= 130


def test_export_dest_time_lag_treats_naive_timestamp_as_utc():
    from datetime import datetime, timedelta

    from viaduck.main import _export_dest_time_lag

    metrics.init("test")  # re-bind: earlier test modules re-init with other pipelines
    delivery = _make_delivery({"dest-2": 7})
    snapshot = delivery.status_snapshot.return_value
    naive = datetime.now(UTC).replace(tzinfo=None) - timedelta(seconds=60)
    with patch("viaduck.main.source.snapshot_times", return_value={7: naive}):
        _export_dest_time_lag(MagicMock(), snapshot, ["dest-2"], snap_now=100)
    val = metrics._dest_time_lag_seconds.labels(pipeline="test", destination="dest-2")._value.get()
    assert 59 <= val <= 70


def test_export_dest_time_lag_skips_never_flushed():
    from viaduck.main import _export_dest_time_lag

    delivery = _make_delivery({"dest-3": 0})
    snapshot = delivery.status_snapshot.return_value
    with patch("viaduck.main.source.snapshot_times") as st:
        _export_dest_time_lag(MagicMock(), snapshot, ["dest-3"], snap_now=100)
    st.assert_not_called()


def test_export_dest_time_lag_never_breaks_the_poll_cycle():
    from viaduck.main import _export_dest_time_lag

    delivery = _make_delivery({"dest-4": 9})
    snapshot = delivery.status_snapshot.return_value
    with patch("viaduck.main.source.snapshot_times", side_effect=RuntimeError("catalog blip")):
        _export_dest_time_lag(MagicMock(), snapshot, ["dest-4"], snap_now=100)  # must not raise


def test_export_dest_time_lag_caught_up_reads_zero():
    """Caught up (cursor == head) => 0, even on a quiet source. Without the
    short-circuit a source with no new commits would grow the gauge without
    bound while dest_lag_snapshots correctly reads 0 in the same state."""
    from viaduck.main import _export_dest_time_lag

    metrics.init("test")
    delivery = _make_delivery({"dest-5": 42})
    snapshot = delivery.status_snapshot.return_value
    with patch("viaduck.main.source.snapshot_times") as st:
        _export_dest_time_lag(MagicMock(), snapshot, ["dest-5"], snap_now=42)
    st.assert_not_called()
    val = metrics._dest_time_lag_seconds.labels(pipeline="test", destination="dest-5")._value.get()
    assert val == 0.0


def test_export_dest_time_lag_one_lookup_for_all_destinations():
    """The headline property: all behind destinations share ONE snapshot_times
    call, with duplicate cursors deduped."""
    from datetime import UTC, datetime, timedelta

    from viaduck.main import _export_dest_time_lag

    metrics.init("test")
    delivery = _make_delivery({"a": 7, "b": 7, "c": 9})
    snapshot = delivery.status_snapshot.return_value
    ts = datetime.now(UTC) - timedelta(seconds=30)
    with patch("viaduck.main.source.snapshot_times", return_value={7: ts, 9: ts}) as st:
        _export_dest_time_lag(MagicMock(), snapshot, ["a", "b", "c"], snap_now=100)
    st.assert_called_once()
    assert st.call_args[0][1] == [7, 9]
    for dest in ("a", "b", "c"):
        val = metrics._dest_time_lag_seconds.labels(pipeline="test", destination=dest)._value.get()
        assert 29 <= val <= 40


def test_export_dest_time_lag_future_timestamp_clamps_to_zero():
    from datetime import UTC, datetime, timedelta

    from viaduck.main import _export_dest_time_lag

    metrics.init("test")
    delivery = _make_delivery({"dest-6": 3})
    snapshot = delivery.status_snapshot.return_value
    future = datetime.now(UTC) + timedelta(seconds=45)  # clock skew
    with patch("viaduck.main.source.snapshot_times", return_value={3: future}):
        _export_dest_time_lag(MagicMock(), snapshot, ["dest-6"], snap_now=100)
    val = metrics._dest_time_lag_seconds.labels(pipeline="test", destination="dest-6")._value.get()
    assert val == 0.0


def test_poll_cycle_wires_time_lag_export():
    """The call-site line in _poll_cycle must exist: mutation testing showed
    the helper alone passes with the wiring deleted."""
    delivery = _make_delivery({"dest-1": 10})
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])

    with (
        patch("viaduck.main.source.snapshot_bounds", return_value=(1, 10)),
        patch("viaduck.main._export_dest_time_lag") as ex,
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1"],
            {"quacksworth": "dest-1"},
            key_columns=[],
            mode="append_only",
        )
    ex.assert_called_once()
    args = ex.call_args[0]
    assert args[2] == ["dest-1"]
    assert args[3] == 10  # snap_now threaded through


# ---------------------------------------------------------------------------
# _clamp_expired_cursors (retention-edge clamp)
# ---------------------------------------------------------------------------


def test_clamp_expired_cursors_clamps_only_below_floor():
    delivery = MagicMock()
    delivery.flushed_snapshots.return_value = {"d1": 10, "d2": 99, "d3": 200}
    delivery.clamp_to_retention.return_value = 10
    assert _clamp_expired_cursors(delivery, ["d1", "d2", "d3"], 100) == set()
    # floor = earliest - 1 = 99: d1 clamps; d2 sits exactly at the floor
    # (its next read starts at the oldest retained snapshot); d3 is ahead.
    delivery.clamp_to_retention.assert_called_once_with("d1", 99)


def test_clamp_expired_cursors_no_snapshots_or_check_failure_is_noop():
    delivery = MagicMock()
    # No earliest snapshot (empty catalog): nothing to clamp against.
    assert _clamp_expired_cursors(delivery, ["d1"], None) == set()
    delivery.flushed_snapshots.assert_not_called()
    # Best-effort: a flushed-snapshot read failure never breaks the cycle.
    delivery.flushed_snapshots.side_effect = RuntimeError("pg down")
    assert _clamp_expired_cursors(delivery, ["d1"], 100) == set()
    delivery.clamp_to_retention.assert_not_called()


def test_clamp_expired_cursors_scopes_to_given_ids():
    # Paused/retired destinations are not readable and must not be clamped
    # behind the operator's back — the clamp catches them on resume.
    delivery = MagicMock()
    delivery.flushed_snapshots.return_value = {"d1": 10, "paused-dest": 5}
    delivery.clamp_to_retention.return_value = 10
    _clamp_expired_cursors(delivery, ["d1"], 100)
    delivery.clamp_to_retention.assert_called_once_with("d1", 99)


def test_clamp_expired_cursors_failure_isolated_and_excludes_id():
    # One candidate's clamp failure must not abort its peers, and the
    # failed id is returned so the cycle excludes it from reads — reading
    # its still-expired cursor would be the fatal path this exists to fix.
    delivery = MagicMock()
    delivery.flushed_snapshots.return_value = {"bad": 5, "good": 10}

    def _clamp(did, floor):
        if did == "bad":
            raise RuntimeError("state store down")
        return 10

    delivery.clamp_to_retention.side_effect = _clamp
    assert _clamp_expired_cursors(delivery, ["bad", "good"], 100) == {"bad"}
    assert delivery.clamp_to_retention.call_count == 2


def test_poll_cycle_clamps_before_reading_and_reads_from_floor():
    # Pins the call-site ordering: the clamp runs BEFORE the plan snapshot,
    # so a clamped destination's read starts at the retention floor THIS
    # cycle — moving the clamp after read_plan() keeps the helper tests
    # green while reintroducing the fatal expired read.
    delivery = _make_delivery({"dest-1": 10})
    delivery.flushed_snapshots.return_value = {"dest-1": 10}

    def _clamp(did, floor):
        # Mirror the real clamp: the plan the poll thread snapshots after
        # this call must serve the clamped position.
        delivery.read_plan.return_value = {did: (floor, 0)}
        return 10

    delivery.clamp_to_retention.side_effect = _clamp
    router = MagicMock()
    router.build_filter_expr.return_value = "company = 'quacksworth'"
    cfg = _make_cfg([("dest-1", "quacksworth")])

    with (
        patch("viaduck.main.source.snapshot_bounds", return_value=(100, 120)),
        patch("viaduck.main.source.read_cdc", return_value=pa.table({"company": pa.array([], type=pa.string())})) as rc,
        patch("viaduck.main._export_dest_time_lag"),
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1"],
            {"quacksworth": "dest-1"},
            key_columns=[],
            mode="append_only",
        )

    delivery.clamp_to_retention.assert_called_once_with("dest-1", 99)
    assert rc.call_args.kwargs["after_snapshot"] == 99


def test_poll_cycle_excludes_destination_whose_clamp_failed():
    delivery = _make_delivery({"dest-1": 10})
    delivery.flushed_snapshots.return_value = {"dest-1": 10}
    delivery.clamp_to_retention.side_effect = RuntimeError("state store down")
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "quacksworth")])

    with (
        patch("viaduck.main.source.snapshot_bounds", return_value=(100, 120)),
        patch("viaduck.main.source.read_cdc") as rc,
        patch("viaduck.main._export_dest_time_lag"),
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1"],
            {"quacksworth": "dest-1"},
            key_columns=[],
            mode="append_only",
        )

    # The expired destination sat the cycle out instead of fataling it.
    rc.assert_not_called()


def test_poll_cycle_contains_group_read_failure():
    # A read/route failure is contained to its group: the cycle completes
    # (no exception to the run loop, which would exit the process) and
    # flush triggers still run.
    delivery = _make_delivery({"dest-1": 10})
    router = MagicMock()
    router.build_filter_expr.return_value = "company = 'quacksworth'"
    cfg = _make_cfg([("dest-1", "quacksworth")])

    with (
        patch("viaduck.main.source.snapshot_bounds", return_value=(1, 120)),
        patch("viaduck.main.source.read_cdc", side_effect=RuntimeError("range expired mid-cycle")),
        patch("viaduck.main._export_dest_time_lag"),
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1"],
            {"quacksworth": "dest-1"},
            key_columns=[],
            mode="append_only",
        )

    delivery.maybe_flush.assert_called()


def test_poll_cycle_survives_membership_smaller_than_assigned():
    # Stage-4 landmine hardening: a destination removed from the delivery
    # manager mid-run (status_snapshot omits it) while still present in
    # the loop's startup-captured assigned_ids must not KeyError the
    # cycle — the run loop treats poll-cycle errors as fatal.
    delivery = _make_delivery({"dest-1": 10})  # dest-2 absent from snapshot
    delivery.flushed_snapshots.return_value = {"dest-1": 10, "dest-2": 10}
    delivery.read_plan.return_value = {"dest-1": (10, 0), "dest-2": (10, 0)}
    router = MagicMock()
    cfg = _make_cfg([("dest-1", "a"), ("dest-2", "b")])

    with (
        patch("viaduck.main.source.snapshot_bounds", return_value=(1, 10)),
        patch("viaduck.main._export_dest_time_lag"),
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1", "dest-2"],
            {"a": "dest-1", "b": "dest-2"},
            key_columns=[],
            mode="append_only",
        )
    delivery.maybe_flush.assert_called()


# ---------------------------------------------------------------------------
# Poll-cycle time budget + group rotation (slow-consumer isolation)
# ---------------------------------------------------------------------------


class _Clock:
    """Stateful fake for time.monotonic: advances only when the test sets
    it, so the cycle budget check trips at deterministic points. The fake
    covers heartbeat threads too (shared module attr) and never raises
    StopIteration, unlike a side_effect list."""

    def __init__(self, now=0.0):
        self.now = now

    def __call__(self):
        return self.now


def _timebox_setup():
    """dest-1 at cursor 3, dest-2 at cursor 5; current snapshot 10. Empty
    routed reads (positions advance, nothing buffers)."""
    delivery = _make_delivery({"dest-1": 3, "dest-2": 5})
    router = MagicMock()
    router.build_filter_expr.side_effect = lambda rvs: "f"
    router.split_and_count.side_effect = lambda table, rvs: ({}, table.num_rows)
    cfg = _make_cfg([("dest-1", "a"), ("dest-2", "b")])
    cfg.poll.interval_seconds = 5.0
    cfg.poll.cycle_time_budget_seconds = 0.0  # derive: 0.8 x 5.0 = 4.0s
    return delivery, router, cfg


def _run_timebox_cycle(delivery, router, cfg, rotation, clock, reads, advance_after_first_read=False):
    def fake_read(src_table, after_snapshot, end_snapshot, **kw):
        reads.append(after_snapshot)
        if advance_after_first_read:
            clock.now = 100.0  # the NEXT group's budget check trips (budget 4.0s)
        return pa.table({"company": ["a"], "value": [1]})

    with (
        patch("viaduck.main.source.snapshot_bounds", return_value=(1, 10)),
        patch("viaduck.main.source.read_cdc", side_effect=fake_read),
        patch("viaduck.main._export_dest_time_lag"),
        patch("viaduck.main.time.monotonic", side_effect=clock),
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1", "dest-2"],
            {"a": "dest-1", "b": "dest-2"},
            key_columns=[],
            mode="append_only",
            rotation=rotation,
        )


def test_timebox_defers_unreached_groups_and_rotation_resumes():
    """Budget trips after the first group: the second defers to the next
    cycle, which starts AT it (rotation), not back at the most-lagging one."""
    delivery, router, cfg = _timebox_setup()
    rotation = {"offset": 0}
    clock = _Clock(0.0)

    reads: list[int] = []
    _run_timebox_cycle(delivery, router, cfg, rotation, clock, reads, advance_after_first_read=True)
    assert reads == [3], f"only the most-lagging group reads before the budget trips, got {reads}"
    assert rotation["offset"] == 1, "next cycle must resume at the deferred group"

    reads.clear()
    clock.now = 0.0
    _run_timebox_cycle(delivery, router, cfg, rotation, clock, reads)
    assert reads == [5, 3], f"deferred group reads first next cycle, got {reads}"
    # Full coverage: offset advanced past both groups; mod-wraps next cycle.
    assert rotation["offset"] % 2 == 1


def test_full_coverage_cycle_processes_every_group_sorted():
    """With the budget never tripping, all groups read every cycle in
    ascending-cursor order (rotation offset 0 preserves legacy behavior)."""
    delivery, router, cfg = _timebox_setup()
    rotation = {"offset": 0}
    clock = _Clock(0.0)
    reads: list[int] = []
    _run_timebox_cycle(delivery, router, cfg, rotation, clock, reads)
    assert reads == [3, 5], f"all groups read in ascending order, got {reads}"


def test_negative_budget_disables_timebox():
    """poll.cycle_time_budget_seconds < 0: legacy unbounded cycles (no
    deferral regardless of elapsed time)."""
    delivery, router, cfg = _timebox_setup()
    cfg.poll.cycle_time_budget_seconds = -1.0
    rotation = {"offset": 0}
    clock = _Clock(1000.0)  # absurdly late — still no timebox
    reads: list[int] = []
    _run_timebox_cycle(delivery, router, cfg, rotation, clock, reads)
    assert reads == [3, 5], f"disabled budget must not defer groups, got {reads}"


def test_timebox_floor_guarantees_one_group_per_cycle():
    """Budget exhausted BEFORE the loop (slow snapshot_bounds / clamp): the
    first group with work still reads — never a zero-progress cycle. The
    deferral only kicks in from the second work-group onward."""
    delivery, router, cfg = _timebox_setup()
    rotation = {"offset": 0}
    clock = _Clock(0.0)
    reads: list[int] = []

    def fake_bounds(table):
        clock.now = 1000.0  # metadata work eats the entire 4.0s budget
        return (1, 10)

    def fake_read(src_table, after_snapshot, end_snapshot, **kw):
        reads.append(after_snapshot)
        return pa.table({"company": ["a"], "value": [1]})

    with (
        patch("viaduck.main.source.snapshot_bounds", side_effect=fake_bounds),
        patch("viaduck.main.source.read_cdc", side_effect=fake_read),
        patch("viaduck.main._export_dest_time_lag"),
        patch("viaduck.main.time.monotonic", side_effect=clock),
    ):
        _poll_cycle(
            MagicMock(),
            delivery,
            MagicMock(),
            router,
            cfg,
            ["dest-1", "dest-2"],
            {"a": "dest-1", "b": "dest-2"},
            key_columns=[],
            mode="append_only",
            rotation=rotation,
        )
    assert reads == [3], f"floor: exactly the first work-group reads despite an eaten budget, got {reads}"
    assert rotation["offset"] == 1
