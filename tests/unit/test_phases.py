"""Unit tests for per-flush phase timing.

Covers the accumulator itself (viaduck/phases.py) and its wiring into the
write path (viaduck/apply.py): what gets timed, what the probe gate does,
and how retries accumulate. The delivery-side wiring — queue_wait measured
from submit, the log line, phases summing to the reported duration — lives
in test_delivery.py next to the rest of the flush-path tests.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from viaduck import metrics
from viaduck.apply import _catalog_probe, _write_with_retry, append_only
from viaduck.phases import (
    ACQUIRE,
    APPEND,
    COLD_ATTACH,
    NESTED_PHASES,
    PARTITION_PHASES,
    PROBE,
    PROJECTION,
    QUEUE_WAIT,
    RETRY_BACKOFF,
    FlushPhases,
)


def setup_module():
    metrics.init("test")


def _batch(n: int = 4) -> pa.Table:
    return pa.table({"company": ["a"] * n, "value": list(range(n))})


class _Pool:
    """Destination pool double that records how it was leased."""

    def __init__(self, projection=None, on_append=None):
        self.projection = projection
        self.on_append = on_append
        self.gets = []
        self.table = MagicMock()
        self.catalog = MagicMock()
        self.catalog.name = "team_2"
        if on_append is not None:
            self.table.append.side_effect = lambda b: on_append()

    def get(self, destination_id, phases=None):
        self.gets.append(phases)
        return (self.catalog, self.table)

    def release(self, destination_id):
        pass

    def evict(self, destination_id):
        pass

    def projection_for(self, destination_id):
        return self.projection


# ---------------------------------------------------------------------------
# The accumulator
# ---------------------------------------------------------------------------


def test_phase_vocabularies_are_disjoint():
    """A nested phase counted as a partition phase would double-count it
    into the flush total — the one arithmetic error this metric can make."""
    assert not set(PARTITION_PHASES) & set(NESTED_PHASES)


def test_add_accumulates_rather_than_overwrites():
    p = FlushPhases()
    p.add(APPEND, 1.5)
    p.add(APPEND, 2.0)
    assert p.get(APPEND) == pytest.approx(3.5)


def test_accounted_excludes_queue_wait_and_nested_phases():
    """`accounted` must be comparable to the reported flush duration:
    queue_wait happens before the duration clock starts, and cold_attach is
    a subset of acquire."""
    p = FlushPhases()
    p.add(QUEUE_WAIT, 30.0)
    p.add(ACQUIRE, 12.0)
    p.add(COLD_ATTACH, 12.0)
    p.add(APPEND, 8.0)
    assert p.accounted() == pytest.approx(20.0)


def test_start_measures_queue_wait_from_submit():
    with patch("viaduck.phases.time.monotonic", side_effect=[100.0]):
        p = FlushPhases(submitted_at=95.5)
        p.start()
    assert p.get(QUEUE_WAIT) == pytest.approx(4.5)


def test_start_without_submit_stamp_records_no_queue_wait():
    p = FlushPhases()
    p.start()
    assert QUEUE_WAIT not in p.recorded()


def test_observe_emits_every_phase_and_the_attempt_count():
    p = FlushPhases()
    p.add(APPEND, 3.0)
    p.add(ACQUIRE, 1.0)
    p.attempts = 4
    with (
        patch.object(metrics, "flush_phase_seconds") as hist,
        patch.object(metrics, "flush_retry_attempts_total") as ctr,
    ):
        p.observe("team-2")
    assert {c.kwargs["phase"] for c in hist.labels.call_args_list} == {APPEND, ACQUIRE}
    ctr.labels.assert_called_once_with(destination="team-2")
    ctr.labels.return_value.inc.assert_called_once_with(4)


# ---------------------------------------------------------------------------
# Log-line rendering
# ---------------------------------------------------------------------------


def test_log_fragment_matches_the_documented_shape():
    p = FlushPhases(probe_enabled=True)
    p.add(QUEUE_WAIT, 0.1)
    p.add(ACQUIRE, 0.04)
    p.add(PROBE, 62.3)
    p.add(APPEND, 18.9)
    p.add("cursor_persist", 0.2)
    assert p.log_fragment() == "queue=0.1 acquire=0.0 probe=62.3 append=18.9 cursor=0.2"


def test_log_fragment_omits_probe_when_disabled():
    p = FlushPhases(probe_enabled=False)
    p.add(QUEUE_WAIT, 0.1)
    p.add(APPEND, 5.0)
    assert "probe" not in p.log_fragment()


def test_log_fragment_keeps_a_stable_minimum_shape():
    """Even a flush that recorded nothing renders the four core fields, so
    a log parser never has to cope with a missing key."""
    assert FlushPhases().log_fragment() == "queue=0.0 acquire=0.0 append=0.0 cursor=0.0"


def test_log_fragment_shows_optional_phases_only_when_they_carry_signal():
    p = FlushPhases()
    assert "projection" not in p.log_fragment()
    assert "backoff" not in p.log_fragment()
    p.add(PROJECTION, 1.2)
    p.add(RETRY_BACKOFF, 30.0)
    assert "projection=1.2" in p.log_fragment()
    assert "backoff=30.0" in p.log_fragment()


def test_log_fragment_flags_a_cold_attach_and_the_attempt_count():
    p = FlushPhases()
    p.add(ACQUIRE, 14.2)
    p.cold_attach = True
    p.attempts = 3
    fragment = p.log_fragment()
    assert "acquire=14.2(cold)" in fragment
    assert "attempts=3" in fragment


def test_log_fragment_omits_attempts_on_a_first_try_success():
    p = FlushPhases()
    p.attempts = 1
    assert "attempts" not in p.log_fragment()


# ---------------------------------------------------------------------------
# Wiring into the write path
# ---------------------------------------------------------------------------


def test_append_only_times_acquire_and_append():
    pool = _Pool()
    p = FlushPhases()
    append_only(pool, "team-2", _batch(), phases=p)
    assert ACQUIRE in p.recorded()
    assert APPEND in p.recorded()
    assert p.attempts == 1


def test_probe_is_off_by_default_and_runs_no_catalog_query():
    """The probe is the only catalog work this instrumentation adds, so
    "off unless asked" is the property that keeps it safe to deploy."""
    pool = _Pool()
    p = FlushPhases()
    append_only(pool, "team-2", _batch(), phases=p)
    assert PROBE not in p.recorded()
    pool.catalog.fetchall.assert_not_called()


def test_probe_when_enabled_queries_the_same_connection_before_the_append():
    order = []
    pool = _Pool(on_append=lambda: order.append("append"))
    pool.catalog.fetchall.side_effect = lambda sql: order.append("probe")
    p = FlushPhases(probe_enabled=True)
    append_only(pool, "team-2", _batch(), phases=p)
    assert order == ["probe", "append"]
    assert PROBE in p.recorded()


def test_probe_reads_max_snapshot_id_not_the_full_snapshot_list():
    """current_snapshot() would fetch and sort every snapshot row — on a
    long-lived catalog that IS a slow query, and the probe would then
    manufacture the stall it exists to detect."""
    catalog = MagicMock()
    catalog.name = "team_2"
    _catalog_probe(catalog)
    sql = catalog.fetchall.call_args[0][0]
    assert sql == 'SELECT max(snapshot_id) FROM "__ducklake_metadata_team_2".ducklake_snapshot'


def test_probe_refuses_a_catalog_name_that_is_not_a_bare_identifier():
    catalog = MagicMock()
    catalog.name = 'x"; DROP TABLE events; --'
    _catalog_probe(catalog)
    catalog.fetchall.assert_not_called()


def test_probe_failure_never_fails_the_flush():
    pool = _Pool()
    pool.catalog.fetchall.side_effect = RuntimeError("catalog unreachable")
    p = FlushPhases(probe_enabled=True)
    assert append_only(pool, "team-2", _batch(), phases=p) == 4


def test_projection_is_timed_separately_from_the_append():
    projection = MagicMock()
    projection.apply.side_effect = lambda b, on_null_fallback=None: b
    pool = _Pool(projection=projection)
    p = FlushPhases()
    append_only(pool, "team-2", _batch(), phases=p)
    assert PROJECTION in p.recorded()


def test_no_projection_records_no_projection_phase():
    p = FlushPhases()
    append_only(_Pool(), "team-2", _batch(), phases=p)
    assert PROJECTION not in p.recorded()


def test_cold_attach_is_recorded_and_flagged_by_the_pool():
    """The pool decides — a pooled hit returns before the create path, so
    only a genuine connection build sets the flag."""
    from viaduck.destination import DestinationPool

    pool = DestinationPool(MagicMock(), MagicMock(), max_open=2)
    p = FlushPhases()
    with patch.object(DestinationPool, "_create", return_value=(MagicMock(), MagicMock(), None)):
        pool.get("team-2", phases=p)
        pool.release("team-2")
        assert p.cold_attach is True
        assert COLD_ATTACH in p.recorded()

        warm = FlushPhases()
        pool.get("team-2", phases=warm)
        assert warm.cold_attach is False
        assert COLD_ATTACH not in warm.recorded()


def test_retries_accumulate_append_backoff_and_the_attempt_count():
    """A retry-storm flush must report five appends plus the sleeps between
    them, not one very slow write."""
    pool = _Pool()
    attempts = []

    def operation(catalog, table):
        attempts.append(1)
        if len(attempts) < 3:
            raise RuntimeError("commit conflict")

    p = FlushPhases()
    with patch("viaduck.apply._backoff_sleep", return_value=False):
        _write_with_retry(pool, "team-2", operation, phases=p)

    assert p.attempts == 3
    assert RETRY_BACKOFF in p.recorded()
    # Every attempt leased the connection, and every lease was timed.
    assert len(pool.gets) == 3
    assert all(g is p for g in pool.gets)


def test_write_path_works_without_a_phase_timer():
    """`phases` is optional everywhere: seeding and any future non-flush
    caller must not have to build one."""
    pool = _Pool()
    assert append_only(pool, "team-2", _batch()) == 4
    assert pool.gets == [None]
