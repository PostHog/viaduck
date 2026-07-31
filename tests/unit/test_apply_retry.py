"""Retry-path classifiers: which write failures force an evict+reconnect.

Regression coverage for the writer OOM leak — a non-connection write error
(read-only /tmp, disk, permission) must NOT close the connection, because
closing mid-write orphans the fork httpfs write-retry buffer (~one flush,
130-160MB native). Only a real connection-death signal may reconnect.
"""

import os
import pathlib
from unittest.mock import patch

import duckdb
import pytest

from viaduck import metrics
from viaduck.apply import (
    _WRITE_MAX_RETRIES,
    _is_connection_error,
    _is_instance_fatal,
    _is_occ_conflict,
    _write_with_retry,
)
from viaduck.source import _CONNECTION_DEFAULTS, sweep_spill_dirs, with_connection_defaults


def setup_module():
    # _write_with_retry touches pipeline-labelled gauges; bind the label the
    # same way test_delivery.py does.
    metrics.init("test")


# The exact two-layer error the team-50689 wedge surfaced (2026-07-16): a
# fork InternalException during commit invalidated the DuckDB instance, and
# every subsequent statement failed with this wrapper until recreate.
_INVALIDATED_MSG = (
    "FATAL Error: Failed to create view '_pyducklake_tmp_append': "
    "Failed: database has been invalidated because of a previous fatal error. "
    "The database must be restarted prior to being used again.\n"
    'Original error: "Failed to commit DuckLake transaction.\n'
    'Calling GetValueInternal on a value that is NULL"'
)


class TestConnectionErrorClassifier:
    def test_readonly_fs_is_not_a_connection_error(self):
        # The exact error behind the 25h team-2 stall + OOM amplifier.
        exc = OSError('IO Error: Failed to create directory ".tmp": Read-only file system')
        assert not _is_connection_error(exc)
        assert not _is_occ_conflict(exc)

    def test_disk_and_permission_errors_do_not_reconnect(self):
        for msg in ("IO Error: No space left on device", "IO Error: Permission denied"):
            assert not _is_connection_error(Exception(msg)), msg

    def test_occ_conflict_is_not_a_connection_error(self):
        exc = Exception("Failed to commit DuckLake transaction: Transaction conflict")
        assert _is_occ_conflict(exc)
        assert not _is_connection_error(exc)

    def test_genuine_connection_deaths_reconnect(self):
        for msg in (
            "server closed the connection unexpectedly",
            "connection to server at 'x' (1.2.3.4), port 5432 failed",
            "could not connect to server",
            "terminating connection due to administrator command",
            "SSL connection has been closed unexpectedly",
            "connection reset by peer",
        ):
            assert _is_connection_error(Exception(msg)), msg


class TestInstanceFatalClassifier:
    def test_invalidated_database_message_is_instance_fatal(self):
        # String fallback: even wrapped in a plain Exception (a future
        # wrapping layer), the invalidated-database text classifies fatal.
        exc = Exception(_INVALIDATED_MSG)
        assert _is_instance_fatal(exc)
        # Not a connection error — this disjointness is WHY the wedge
        # happened: the connection allowlist alone never evicted the dead
        # instance and all 15 attempts failed identically.
        assert not _is_connection_error(exc)

    def test_typed_duckdb_fatal_classes_are_instance_fatal(self):
        # pyducklake propagates duckdb exceptions unwrapped; Internal- and
        # Fatal-class errors invalidate the instance the moment they're
        # raised, so the TRIGGERING attempt already classifies fatal and
        # evicts — no wasted same-instance retry.
        internal = duckdb.InternalException("Calling GetValueInternal on a value that is NULL")
        fatal = duckdb.FatalException("anything")
        assert _is_instance_fatal(internal)
        assert _is_instance_fatal(fatal)
        # OOM and transaction conflicts are NOT Fatal/Internal subclasses —
        # they must keep retrying in place, never evict-churn.
        assert not _is_instance_fatal(duckdb.OutOfMemoryException("out of memory"))
        assert not _is_instance_fatal(duckdb.TransactionException("Transaction conflict"))

    def test_untyped_commit_fatal_text_classifies_as_occ(self):
        # If a wrapping layer ever strips the exception type, the original
        # commit fatal's text starts with the OCC marker and retries in
        # place — one wasted attempt; the NEXT attempt hits the invalidated
        # wrapper (string fallback above) and evicts. Documented cost of the
        # string fallback, not a bug.
        exc = Exception("Failed to commit DuckLake transaction.\nCalling GetValueInternal on a value that is NULL")
        assert _is_occ_conflict(exc)
        assert not _is_instance_fatal(exc)

    def test_invalidated_message_overlaps_occ_marker_documented(self):
        # The invalidated wrapper EMBEDS the original commit error, so it
        # also matches _OCC_CONFLICT_MARKERS. Harmless only because the
        # retry loop consults instance-fatal for evict/fail-fast and never
        # branches on _is_occ_conflict — if OCC-first branching is ever
        # reintroduced, this overlap re-creates the wedge.
        exc = Exception(_INVALIDATED_MSG)
        assert _is_occ_conflict(exc)
        assert _is_instance_fatal(exc)

    def test_connection_deaths_are_not_instance_fatal(self):
        # Disjoint classifiers: either alone triggers evict at the call
        # site, but neither should shadow the other's signature.
        for msg in (
            "server closed the connection unexpectedly",
            "connection reset by peer",
        ):
            assert not _is_instance_fatal(Exception(msg)), msg


class _EvictRecordingPool:
    """Minimal dest_pool double for the retry loop: records evictions and
    counts attempts; the supplied operation decides per-attempt behavior."""

    def __init__(self):
        self.evictions = 0

    def get(self, destination_id):
        return (object(), object())

    def release(self, destination_id):
        pass

    def evict(self, destination_id):
        self.evictions += 1


class TestRetryLoopEviction:
    """Call-site wiring: the classifiers only matter if the retry loop
    actually consults them before deciding to evict / fail fast."""

    def _run(self, operation):
        pool = _EvictRecordingPool()
        with patch("viaduck.apply._backoff_sleep", return_value=False):
            with pytest.raises(RuntimeError):
                _write_with_retry(pool, "team-test", operation)
        return pool

    def _always_fail(self, error_msg):
        def operation(catalog, table):
            raise RuntimeError(error_msg)

        return operation

    def test_persistent_instance_fatal_evicts_once_then_fails_fast(self):
        attempts = []

        def operation(catalog, table):
            attempts.append(1)
            raise RuntimeError(_INVALIDATED_MSG)

        pool = self._run(operation)
        # First fatal: evict + one fresh-instance retry (heals transients).
        # Second fatal on the FRESH instance proves determinism: re-raise
        # immediately instead of burning the remaining OCC-tuned budget on
        # ATTACH + backoff + the known native close-leak per extra attempt.
        # Zero evictions here reproduces the team-50689 wedge; 14 evictions
        # reproduces the evict-churn OOM the reviews flagged.
        assert pool.evictions == 1
        assert len(attempts) == 2

    def test_transient_instance_fatal_heals_on_fresh_instance(self):
        attempts = []

        def operation(catalog, table):
            attempts.append(1)
            if len(attempts) == 1:
                raise RuntimeError(_INVALIDATED_MSG)
            return "ok"

        pool = _EvictRecordingPool()
        with patch("viaduck.apply._backoff_sleep", return_value=False):
            assert _write_with_retry(pool, "team-test", operation) == "ok"
        assert pool.evictions == 1
        assert len(attempts) == 2

    def test_typed_internal_exception_evicts_on_triggering_attempt(self):
        # The typed path: the commit attempt that RAISES InternalException
        # already evicts (instance is invalidated the moment it's raised) —
        # no wasted same-instance retry, unlike the string-only fallback.
        attempts = []

        def operation(catalog, table):
            attempts.append(1)
            if len(attempts) == 1:
                raise duckdb.InternalException("Calling GetValueInternal on a value that is NULL")
            return "ok"

        pool = _EvictRecordingPool()
        with patch("viaduck.apply._backoff_sleep", return_value=False):
            assert _write_with_retry(pool, "team-test", operation) == "ok"
        assert pool.evictions == 1

    def test_occ_conflict_never_evicts_and_uses_full_budget(self):
        pool = self._run(self._always_fail("Failed to commit DuckLake transaction: Transaction conflict"))
        assert pool.evictions == 0

    def test_io_error_never_evicts(self):
        pool = self._run(self._always_fail('IO Error: Failed to create directory ".tmp": Read-only file system'))
        assert pool.evictions == 0

    def test_connection_error_evicts_every_attempt(self):
        # Connection deaths keep the pre-existing behavior: evict on every
        # non-final attempt (no fail-fast cap — reconnects genuinely can
        # heal on any attempt as the network/RDS recovers).
        pool = self._run(self._always_fail("server closed the connection unexpectedly"))
        assert pool.evictions == _WRITE_MAX_RETRIES - 1


class TestTempDirectoryIsolation:
    # DuckDB spill filenames carry no instance token: two embedded instances
    # sharing one temp_directory collide on the same spill files and crash
    # the process (2026-07-29 crash-loop). with_connection_defaults() must
    # therefore NEVER hand two connections the same temp_directory.

    def test_default_base_is_writable_tmp(self):
        # readOnlyRootFilesystem + CWD '/' => the spill BASE must be the
        # mounted writable /tmp emptyDir, not '.tmp' relative to CWD.
        assert _CONNECTION_DEFAULTS.get("temp_directory") == "/tmp"

    def test_each_connection_gets_unique_existing_dir(self, tmp_path):
        a = with_connection_defaults({"temp_directory": str(tmp_path)}, name="dest-a")
        b = with_connection_defaults({"temp_directory": str(tmp_path)}, name="dest-a")
        assert a["temp_directory"] != b["temp_directory"]
        assert os.path.isdir(a["temp_directory"])
        assert os.path.isdir(b["temp_directory"])

    def test_user_temp_directory_is_base_not_literal(self, tmp_path):
        merged = with_connection_defaults({"temp_directory": str(tmp_path)}, name="x")
        assert merged["temp_directory"] != str(tmp_path)
        assert merged["temp_directory"].startswith(str(tmp_path / "viaduck-spill") + os.sep)

    def test_name_is_sanitized_for_filesystem(self, tmp_path):
        merged = with_connection_defaults({"temp_directory": str(tmp_path)}, name="org/1:evil name")
        leaf = os.path.basename(merged["temp_directory"])
        assert "/" not in leaf and ":" not in leaf and " " not in leaf
        assert leaf.startswith("org_1_evil_name-")

    def test_other_defaults_and_user_overrides_untouched(self, tmp_path):
        merged = with_connection_defaults({"temp_directory": str(tmp_path), "pg_connection_limit": "8"}, name="x")
        assert merged["pg_connection_limit"] == "8"
        assert merged["enable_external_file_cache"] == "false"

    def test_sweep_removes_all_spill_dirs(self, tmp_path):
        merged = with_connection_defaults({"temp_directory": str(tmp_path)}, name="x")
        # Simulate crash leftovers: a real file inside a spill dir.
        (pathlib.Path(merged["temp_directory"]) / "duckdb_temp_storage_DEFAULT-0.tmp").write_bytes(b"x")
        sweep_spill_dirs(base=str(tmp_path))
        assert not (tmp_path / "viaduck-spill").exists()
        # Next connection recreates the root cleanly.
        again = with_connection_defaults({"temp_directory": str(tmp_path)}, name="y")
        assert os.path.isdir(again["temp_directory"])

    def test_sweep_noop_when_root_absent(self, tmp_path):
        sweep_spill_dirs(base=str(tmp_path))  # must not raise


class TestFlushDeadline:
    """Overall flush deadline (delivery.flush_deadline_seconds): bounds the
    retry loop's WALL time — the attempt budget alone permits ~5.5 min of
    backoff sleeps plus per-attempt write time, so one pathological
    destination could hold a shared flush worker indefinitely. The deadline
    aborts the loop with FlushDeadlineExceeded (FlushFail semantics)."""

    def test_expired_deadline_raises_before_first_attempt(self):
        from viaduck.apply import FlushDeadlineExceeded

        pool = _EvictRecordingPool()
        calls = []

        def operation(catalog, table):
            calls.append(1)

        with pytest.raises(FlushDeadlineExceeded, match="flush deadline exceeded"):
            _write_with_retry(pool, "team-test", operation, deadline=0.0)  # 1970 — always expired
        assert calls == [], "no attempt may start once the deadline has passed"

    def test_deadline_aborts_mid_retry(self):
        from viaduck.apply import FlushDeadlineExceeded

        pool = _EvictRecordingPool()
        attempts = []

        def operation(catalog, table):
            attempts.append(1)
            raise RuntimeError("Failed to commit DuckLake transaction: Transaction conflict")

        # Clock script: loop-top check (attempt 1) -> delay-cap read ->
        # loop-top check (attempt 2, past deadline).
        with (
            patch("viaduck.apply._backoff_sleep", return_value=False),
            patch("viaduck.apply.time.monotonic", side_effect=[10.0, 10.5, 200.0]),
        ):
            with pytest.raises(FlushDeadlineExceeded):
                _write_with_retry(pool, "team-test", operation, deadline=100.0)
        assert len(attempts) == 1, "deadline must stop the loop after the first failed attempt"
        assert pool.evictions == 0, "an OCC-conflict failure keeps its no-evict policy under the deadline"

    def test_successful_attempt_unaffected_by_deadline(self):
        pool = _EvictRecordingPool()

        def operation(catalog, table):
            return "ok"

        with patch("viaduck.apply.time.monotonic", return_value=10.0):
            assert _write_with_retry(pool, "team-test", operation, deadline=100.0) == "ok"

    def test_no_deadline_preserves_attempt_budget(self):
        pool = _EvictRecordingPool()
        attempts = []

        def operation(catalog, table):
            attempts.append(1)
            raise RuntimeError("perma-fail")

        with patch("viaduck.apply._backoff_sleep", return_value=False):
            with pytest.raises(RuntimeError, match="perma-fail"):
                _write_with_retry(pool, "team-test", operation, deadline=None)
        assert len(attempts) == _WRITE_MAX_RETRIES

    def test_backoff_sleep_capped_at_remaining_deadline(self):
        """A backoff that would carry the flush past the deadline is cut
        short; the loop-top check then raises instead of starting another
        attempt."""
        pool = _EvictRecordingPool()
        sleeps = []

        def operation(catalog, table):
            raise RuntimeError("perma-fail")

        # deadline - now = 1s remaining; computed backoff would be 1.5s
        # (base=1.0 x 2^0 with jitter pinned to +50% — always >1s).
        with (
            patch("viaduck.apply.time.monotonic", side_effect=[10.0, 10.0, 11.5]),
            patch("viaduck.apply.random.uniform", return_value=0.5),
            patch("viaduck.apply._backoff_sleep", side_effect=lambda delay, ev: sleeps.append(delay) or False),
        ):
            from viaduck.apply import FlushDeadlineExceeded

            with pytest.raises(FlushDeadlineExceeded):
                _write_with_retry(pool, "team-test", operation, deadline=11.0)
        assert sleeps == [1.0], f"backoff must be capped at the remaining deadline, got {sleeps}"
