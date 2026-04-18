"""Tests for LRU destination connection pool."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from viaduck import metrics
from viaduck.destination import DestinationPool


def setup_module():
    metrics.init("test")


@pytest.fixture()
def pool():
    config = MagicMock()
    return DestinationPool(config, max_open=3)


# --- Basic pool operations ---


def test_pool_starts_empty(pool):
    assert pool.size == 0


def test_pool_get_creates_connection(pool):
    mock_catalog = MagicMock()
    mock_table = MagicMock()

    with patch.object(pool, "_create", return_value=(mock_catalog, mock_table)):
        cat, tbl = pool.get("team-123")

    assert cat is mock_catalog
    assert tbl is mock_table
    assert pool.size == 1


def test_pool_get_returns_cached(pool):
    mock_catalog = MagicMock()
    mock_table = MagicMock()

    with patch.object(pool, "_create", return_value=(mock_catalog, mock_table)):
        pool.get("team-123")
        cat2, tbl2 = pool.get("team-123")

    assert cat2 is mock_catalog
    assert pool.size == 1


# --- LRU eviction ---


def test_pool_evicts_lru_when_full(pool):
    catalogs = {}

    def mock_create(dest_id):
        cat = MagicMock()
        tbl = MagicMock()
        catalogs[dest_id] = cat
        return cat, tbl

    with patch.object(pool, "_create", side_effect=mock_create):
        pool.get("a")
        pool.get("b")
        pool.get("c")
        assert pool.size == 3

        pool.get("d")  # should evict "a"
        assert pool.size == 3

    catalogs["a"].close.assert_called_once()


def test_pool_lru_order_updated_on_access(pool):
    catalogs = {}

    def mock_create(dest_id):
        cat = MagicMock()
        tbl = MagicMock()
        catalogs[dest_id] = cat
        return cat, tbl

    with patch.object(pool, "_create", side_effect=mock_create):
        pool.get("a")
        pool.get("b")
        pool.get("c")
        pool.get("a")  # move "a" to end
        pool.get("d")  # should evict "b" (the actual LRU)

    catalogs["b"].close.assert_called_once()
    catalogs["a"].close.assert_not_called()


# --- Eviction and close ---


def test_pool_evict_removes_connection(pool):
    mock_catalog = MagicMock()

    with patch.object(pool, "_create", return_value=(mock_catalog, MagicMock())):
        pool.get("team-123")

    pool.evict("team-123")
    assert pool.size == 0
    mock_catalog.close.assert_called_once()


def test_pool_evict_nonexistent_is_noop(pool):
    pool.evict("nonexistent")
    assert pool.size == 0


def test_pool_close_all(pool):
    catalogs = {}

    def mock_create(dest_id):
        cat = MagicMock()
        catalogs[dest_id] = cat
        return cat, MagicMock()

    with patch.object(pool, "_create", side_effect=mock_create):
        pool.get("a")
        pool.get("b")

    pool.close_all()
    assert pool.size == 0
    catalogs["a"].close.assert_called_once()
    catalogs["b"].close.assert_called_once()


# --- Error handling (H4, H7) ---


def test_pool_max_open_zero_raises():
    """max_open < 1 should be rejected at construction (H7)."""
    with pytest.raises(ValueError, match="max_open"):
        DestinationPool(MagicMock(), max_open=0)


def test_pool_max_open_one_works():
    """max_open=1 should work: evict on every new connection."""
    pool = DestinationPool(MagicMock(), max_open=1)
    catalogs = {}

    def mock_create(dest_id):
        cat = MagicMock()
        catalogs[dest_id] = cat
        return cat, MagicMock()

    with patch.object(pool, "_create", side_effect=mock_create):
        pool.get("a")
        assert pool.size == 1
        pool.get("b")  # evicts "a"
        assert pool.size == 1

    catalogs["a"].close.assert_called_once()


def test_pool_create_failure_doesnt_cache(pool):
    """If _create() raises, the failed connection should not be cached (H4)."""
    with patch.object(pool, "_create", side_effect=RuntimeError("connection failed")):
        with pytest.raises(RuntimeError, match="connection failed"):
            pool.get("team-123")

    assert pool.size == 0


def test_pool_eviction_close_failure_continues(pool):
    """If close() throws during eviction, it should not prevent the new connection."""
    calls = []

    def mock_create(dest_id):
        cat = MagicMock()
        if dest_id == "a":
            cat.close.side_effect = RuntimeError("close failed")
        calls.append(dest_id)
        return cat, MagicMock()

    with patch.object(pool, "_create", side_effect=mock_create):
        pool.get("a")
        pool.get("b")
        pool.get("c")
        # Evicting "a" will fail on close(), but "d" should still be added
        pool.get("d")
        assert pool.size == 3


# --- Source schema caching (H2) ---


def test_pool_set_source_schema():
    config = MagicMock()
    pool = DestinationPool(config, max_open=50)
    mock_schema = MagicMock()
    pool.set_source_schema(mock_schema)
    assert pool._source_schema is mock_schema


def test_pool_get_source_schema_cached():
    config = MagicMock()
    pool = DestinationPool(config, max_open=50)
    mock_schema = MagicMock()
    pool._source_schema = mock_schema
    assert pool._get_source_schema() is mock_schema


# --- LRU correctness at scale ---


def test_pool_lru_correctness_at_scale():
    """100 destinations cycling through 10 slots: verify eviction order and counts.

    Tests OrderedDict LRU logic, not connection open/close latency (which is
    DuckDB-bound at ~50-100ms per Catalog).
    """
    pool = DestinationPool(MagicMock(), max_open=10)
    mock_catalog = MagicMock()

    with patch.object(pool, "_create", return_value=(mock_catalog, MagicMock())):
        for i in range(100):
            pool.get(f"dest-{i}")

    assert pool.size == 10
    for i in range(90, 100):
        assert f"dest-{i}" in pool._pool
    for i in range(90):
        assert f"dest-{i}" not in pool._pool
    assert mock_catalog.close.call_count == 90
