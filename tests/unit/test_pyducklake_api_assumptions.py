"""Tests that codify viaduck's assumptions about pyducklake's API surface.

If pyducklake changes one of these — e.g. flips a property to a method or vice
versa — we want to know at `pytest` time, not at first-deploy-in-production
time. This file is the canary for the class of bug where viaduck calls a
property as a method (or vice versa). MagicMock-based unit tests cannot catch
that mismatch because the auto-generated attribute is itself callable; only
checking the actual descriptor type does.

The viaduck pod crashed in mw-dev with `TypeError: 'Schema' object is not
callable` because two callsites did `tbl.schema()` against pyducklake's
`@property` Table.schema. Every property access we rely on now has a test
here.
"""

from __future__ import annotations

import inspect


def test_pyducklake_table_schema_is_a_property():
    """`Table.schema` must be a @property — viaduck reads it as `tbl.schema`."""
    from pyducklake.table import Table

    descriptor = inspect.getattr_static(Table, "schema")
    assert isinstance(descriptor, property), (
        "pyducklake.Table.schema is no longer a @property. Audit viaduck/main.py "
        "and viaduck/destination.py — any `.schema` attribute access may need to "
        "become a method call (or vice versa)."
    )


def test_pyducklake_table_current_snapshot_is_callable():
    """`Table.current_snapshot()` must be callable — viaduck invokes it in source.py."""
    from pyducklake.table import Table

    descriptor = inspect.getattr_static(Table, "current_snapshot")
    assert not isinstance(descriptor, property), (
        "pyducklake.Table.current_snapshot became a @property. viaduck/source.py "
        "calls it as a method — drop the parens."
    )
    assert callable(descriptor)


def test_pyducklake_table_snapshots_is_callable():
    """`Table.snapshots()` must be callable — viaduck invokes it transitively
    via `current_snapshot()`.
    """
    from pyducklake.table import Table

    descriptor = inspect.getattr_static(Table, "snapshots")
    assert not isinstance(descriptor, property)
    assert callable(descriptor)


def test_pyducklake_catalog_load_table_is_callable():
    """`Catalog.load_table()` must be callable — viaduck uses it in source/destination."""
    from pyducklake.catalog import Catalog

    descriptor = inspect.getattr_static(Catalog, "load_table")
    assert not isinstance(descriptor, property)
    assert callable(descriptor)


def test_pyducklake_catalog_close_is_callable():
    """`Catalog.close()` must be callable — viaduck uses it in destination cleanup."""
    from pyducklake.catalog import Catalog

    descriptor = inspect.getattr_static(Catalog, "close")
    assert not isinstance(descriptor, property)
    assert callable(descriptor)
