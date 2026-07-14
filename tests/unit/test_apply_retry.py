"""Retry-path classifiers: which write failures force an evict+reconnect.

Regression coverage for the writer OOM leak — a non-connection write error
(read-only /tmp, disk, permission) must NOT close the connection, because
closing mid-write orphans the fork httpfs write-retry buffer (~one flush,
130-160MB native). Only a real connection-death signal may reconnect.
"""

from viaduck.apply import _is_connection_error, _is_occ_conflict
from viaduck.source import _CONNECTION_DEFAULTS, with_connection_defaults


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


class TestTempDirectoryDefault:
    def test_default_points_at_writable_tmp(self):
        # readOnlyRootFilesystem + CWD '/' => DuckDB must spill to the mounted
        # writable /tmp emptyDir, not '.tmp' relative to the read-only CWD.
        assert _CONNECTION_DEFAULTS.get("temp_directory") == "/tmp"

    def test_default_applied_when_absent(self):
        assert with_connection_defaults({})["temp_directory"] == "/tmp"

    def test_user_property_overrides_default(self):
        assert with_connection_defaults({"temp_directory": "/custom"})["temp_directory"] == "/custom"
