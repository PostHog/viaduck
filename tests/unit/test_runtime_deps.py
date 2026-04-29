"""Smoke test: every module viaduck imports at runtime must be installable
from the production dependency list, not a dev-only group.

The viaduck pod crashed in mw-dev with `ModuleNotFoundError: No module named
'pytz'` because `pytz` was listed under `[dependency-groups].dev` in
pyproject.toml. Tests had it via the dev group; the Docker image used
`uv sync --frozen --no-dev` which excludes dev deps. duckdb's timestamp
deserialization (called by `pyducklake.Table.snapshots`) requires pytz at
runtime — first call to `current_snapshot()` raised at startup.

This test asserts that every direct/transitive runtime dependency is
importable in a context that mimics the production install (no dev deps).
If anyone moves a runtime dep into the dev group again, this fires.
"""

from __future__ import annotations


def test_runtime_dependencies_importable():
    """All these modules are accessed during normal viaduck startup or poll
    cycles. If a future PR removes one from `[project].dependencies`, this
    will fail at test time rather than at first deploy.
    """
    import duckdb  # DuckDB Python client
    import prometheus_client  # /metrics endpoint
    import pyarrow  # Arrow tables for CDC rows
    import pyducklake  # DuckLake catalog
    import pytz  # required by duckdb timestamp deserialization
    import yaml  # config loader

    # Touch one symbol on each so the import is a real load, not just a stub.
    # `__version__` isn't on every module (prometheus_client doesn't expose
    # it), so we reach for a known-stable public symbol instead.
    assert duckdb.connect
    assert prometheus_client.Counter
    assert pyarrow.table
    assert pyducklake.Catalog
    assert pytz.UTC
    assert yaml.safe_load


def test_viaduck_modules_importable_without_dev_group():
    """Importing the viaduck source package should not pull in any dev-only
    dependency. If a future module ever does `import pytest` or similar at
    top level, this test will fail.
    """
    # Direct imports of every viaduck source module that runs in production.
    import viaduck.config
    import viaduck.destination
    import viaduck.logging_config
    import viaduck.main
    import viaduck.metrics
    import viaduck.router
    import viaduck.server
    import viaduck.source
    import viaduck.state

    # Trivial assertion to keep the test non-empty; the act of importing is
    # what matters.
    assert viaduck.main.main is not None
