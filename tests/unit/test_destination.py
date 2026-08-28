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
    return DestinationPool(config, MagicMock(), max_open=3)


# --- Basic pool operations ---


def test_pool_starts_empty(pool):
    assert pool.size == 0


def test_pool_get_creates_connection(pool):
    mock_catalog = MagicMock()
    mock_table = MagicMock()

    with patch.object(pool, "_create", return_value=(mock_catalog, mock_table, None)):
        cat, tbl = pool.get("team-123")

    assert cat is mock_catalog
    assert tbl is mock_table
    assert pool.size == 1


def test_pool_get_returns_cached(pool):
    mock_catalog = MagicMock()
    mock_table = MagicMock()

    with patch.object(pool, "_create", return_value=(mock_catalog, mock_table, None)):
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
        return cat, tbl, None

    with patch.object(pool, "_create", side_effect=mock_create):
        for d in ("a", "b", "c"):
            pool.get(d)
            pool.release(d)
        assert pool.size == 3

        pool.get("d")  # should evict "a"
        pool.release("d")
        assert pool.size == 3

    catalogs["a"].close.assert_called_once()


def test_pool_lru_order_updated_on_access(pool):
    catalogs = {}

    def mock_create(dest_id):
        cat = MagicMock()
        tbl = MagicMock()
        catalogs[dest_id] = cat
        return cat, tbl, None

    with patch.object(pool, "_create", side_effect=mock_create):
        for d in ("a", "b", "c", "a", "d"):
            pool.get(d)  # "a" twice: moves to MRU; "d" evicts "b" (true LRU)
            pool.release(d)

    catalogs["b"].close.assert_called_once()
    catalogs["a"].close.assert_not_called()


# --- Eviction and close ---


def test_pool_evict_removes_connection(pool):
    mock_catalog = MagicMock()

    with patch.object(pool, "_create", return_value=(mock_catalog, MagicMock(), None)):
        pool.get("team-123")
        pool.release("team-123")

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
        return cat, MagicMock(), None

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
        DestinationPool(MagicMock(), MagicMock(), max_open=0)


def test_pool_max_open_one_works():
    """max_open=1 should work: evict on every new connection."""
    pool = DestinationPool(MagicMock(), MagicMock(), max_open=1)
    catalogs = {}

    def mock_create(dest_id):
        cat = MagicMock()
        catalogs[dest_id] = cat
        return cat, MagicMock(), None

    with patch.object(pool, "_create", side_effect=mock_create):
        pool.get("a")
        pool.release("a")
        assert pool.size == 1
        pool.get("b")  # evicts "a"
        pool.release("b")
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
        return cat, MagicMock(), None

    with patch.object(pool, "_create", side_effect=mock_create):
        for d in ("a", "b", "c"):
            pool.get(d)
            pool.release(d)
        # Evicting "a" will fail on close(), but "d" should still be added
        pool.get("d")
        pool.release("d")
        assert pool.size == 3


# --- Source schema caching (H2) ---


def test_pool_set_source_schema():
    config = MagicMock()
    pool = DestinationPool(config, MagicMock(), max_open=50)
    mock_schema = MagicMock()
    pool.set_source_schema(mock_schema)
    assert pool._source_schema is mock_schema


def test_pool_get_source_schema_cached():
    config = MagicMock()
    pool = DestinationPool(config, MagicMock(), max_open=50)
    mock_schema = MagicMock()
    pool._source_schema = mock_schema
    assert pool._get_source_schema() is mock_schema


def test_pool_get_source_schema_live_treats_schema_as_property():
    """Regression: pyducklake `Table.schema` is a @property, not a method.

    Earlier versions called it as `src_tbl.schema()` which raised
    `TypeError: 'Schema' object is not callable` at runtime — invisible to
    MagicMock-based tests because `mock.schema()` happily returns another
    Mock. This test uses a non-callable schema double so calling it as a
    method would fail loudly.
    """

    class _FakeTable:
        # `schema` is a plain attribute — accessing as `.schema` returns the
        # value; calling as `.schema()` raises TypeError on the value.
        def __init__(self, schema):
            self.schema = schema

    config = MagicMock()
    config.source.name = "source"
    config.source.postgres_uri_env = "SRC_PG"
    config.source.postgres_uri = "postgres:host=localhost"
    config.source.data_path = "/tmp/data"
    config.source.table = "events"
    config.source.resolved_properties.return_value = {}

    fake_schema = "non-callable-sentinel"
    fake_table = _FakeTable(schema=fake_schema)
    fake_catalog = MagicMock()
    fake_catalog.load_table.return_value = fake_table

    pool = DestinationPool(config, MagicMock(), max_open=50)
    pool._source_schema = None

    with patch("pyducklake.Catalog", return_value=fake_catalog):
        result = pool._get_source_schema()

    assert result == fake_schema
    fake_catalog.close.assert_called_once()


# --- LRU correctness at scale ---


def test_pool_lru_correctness_at_scale():
    """100 destinations cycling through 10 slots: verify eviction order and counts.

    Tests OrderedDict LRU logic, not connection open/close latency (which is
    DuckDB-bound at ~50-100ms per Catalog).
    """
    pool = DestinationPool(MagicMock(), MagicMock(), max_open=10)
    mock_catalog = MagicMock()

    with patch.object(pool, "_create", return_value=(mock_catalog, MagicMock(), None)):
        for i in range(100):
            pool.get(f"dest-{i}")
            pool.release(f"dest-{i}")

    assert pool.size == 10
    for i in range(90, 100):
        assert f"dest-{i}" in pool._pool
    for i in range(90):
        assert f"dest-{i}" not in pool._pool
    assert mock_catalog.close.call_count == 90


# --- Lease/pinning semantics (buffered-delivery worker pool) ---


def test_pool_pinned_entry_not_lru_evicted():
    """A pinned (leased, unreleased) entry must survive LRU pressure."""
    pool = DestinationPool(MagicMock(), MagicMock(), max_open=2)
    catalogs = {}

    def mock_create(dest_id):
        cat = MagicMock()
        catalogs[dest_id] = cat
        return cat, MagicMock(), None

    with patch.object(pool, "_create", side_effect=mock_create):
        pool.get("pinned")  # NOT released — a worker mid-transaction
        pool.get("b")
        pool.release("b")
        pool.get("c")  # at capacity: must evict "b", not the pinned entry
        pool.release("c")

    catalogs["pinned"].close.assert_not_called()
    catalogs["b"].close.assert_called_once()
    pool.release("pinned")


def test_pool_evict_while_pinned_defers_close():
    """Force-evict of a pinned entry defers the close to the final release."""
    pool = DestinationPool(MagicMock(), MagicMock(), max_open=3)
    cat = MagicMock()

    with patch.object(pool, "_create", return_value=(cat, MagicMock(), None)):
        pool.get("d1")  # pinned

    pool.evict("d1")
    cat.close.assert_not_called()  # still leased
    pool.release("d1")
    cat.close.assert_called_once()  # closed at final release


def test_pool_projection_stored_with_pool_entry(pool):
    """When _create returns a projection plan, it must be stored into
    _projections in the same critical section as the _pool insert. Otherwise
    a concurrent evict() between the two mutations leaves _pool populated but
    _projections empty → identity fall-through → positional-insert corruption.
    """
    from viaduck.schema_projection import ProjectionPlan

    fake_plan = ProjectionPlan(
        target_schema=MagicMock(),
        source_column_order=("a", "b"),
        passthrough_columns=("a", "b"),
    )
    with patch.object(pool, "_create", return_value=(MagicMock(), MagicMock(), fake_plan)):
        pool.get("d1")
    assert pool._projections["d1"] is fake_plan
    pool.release("d1")


def test_pool_projection_dropped_on_evict(pool):
    """evict() must pop _projections alongside _pool so a subsequent _create
    for the same dest rebuilds the plan against the current schema."""
    from viaduck.schema_projection import ProjectionPlan

    fake_plan = ProjectionPlan(
        target_schema=MagicMock(),
        source_column_order=("a",),
        passthrough_columns=("a",),
    )
    with patch.object(pool, "_create", return_value=(MagicMock(), MagicMock(), fake_plan)):
        pool.get("d1")
        pool.release("d1")
    assert "d1" in pool._projections

    pool.evict("d1")
    assert "d1" not in pool._projections


def test_pool_projection_dropped_on_lru_eviction(pool):
    """LRU-eviction path in get() also pops _projections for the victim."""
    from viaduck.schema_projection import ProjectionPlan

    def _mk_plan():
        return ProjectionPlan(
            target_schema=MagicMock(),
            source_column_order=("a",),
            passthrough_columns=("a",),
        )

    def mock_create(dest_id):
        return MagicMock(), MagicMock(), _mk_plan()

    with patch.object(pool, "_create", side_effect=mock_create):
        for d in ("a", "b", "c"):
            pool.get(d)
            pool.release(d)
        for d in ("a", "b", "c"):
            assert d in pool._projections

        pool.get("d")  # evicts "a"
        pool.release("d")

    assert "a" not in pool._projections
    for d in ("b", "c", "d"):
        assert d in pool._projections


def test_pool_projection_cleared_on_close_all(pool):
    from viaduck.schema_projection import ProjectionPlan

    fake_plan = ProjectionPlan(
        target_schema=MagicMock(),
        source_column_order=("a",),
        passthrough_columns=("a",),
    )
    with patch.object(pool, "_create", return_value=(MagicMock(), MagicMock(), fake_plan)):
        pool.get("d1")
        pool.release("d1")
    assert pool._projections

    pool.close_all()
    assert not pool._projections


def test_pool_projection_stress_concurrent_get_release(pool):
    """Threaded stress: N workers hammering get/release across M destinations
    with plans attached. `projection_for()` must never return None for a dest
    whose pool entry we just acquired (the H3 race — mutating _projections
    outside the lock could drop a plan mid-flight)."""
    import threading

    from viaduck.schema_projection import ProjectionPlan

    def _mk_plan():
        return ProjectionPlan(
            target_schema=MagicMock(),
            source_column_order=("a",),
            passthrough_columns=("a",),
        )

    def mock_create(dest_id):
        return MagicMock(), MagicMock(), _mk_plan()

    errors: list[str] = []
    stop = threading.Event()

    def worker(i):
        try:
            for _ in range(50):
                if stop.is_set():
                    return
                dest = f"d{i % 5}"
                pool.get(dest)
                if pool.projection_for(dest) is None:
                    errors.append(f"plan missing for {dest}")
                pool.release(dest)
        except Exception as e:
            errors.append(str(e))

    with patch.object(pool, "_create", side_effect=mock_create):
        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

    stop.set()
    assert not errors, f"race conditions observed: {errors[:5]}"


def test_pool_all_pinned_overshoots_instead_of_deadlock():
    pool = DestinationPool(MagicMock(), MagicMock(), max_open=1)
    with patch.object(pool, "_create", side_effect=lambda d: (MagicMock(), MagicMock(), None)):
        pool.get("a")  # pinned
        pool.get("b")  # capacity exceeded but "a" is pinned -> overshoot
        assert pool.size == 2
    pool.release("a")
    pool.release("b")


# ----------------------------------------------------------------------
# Partition spec application — _ensure_partition_spec
# ----------------------------------------------------------------------


def _make_dest_cfg(partition_by=(), partition_by_allow_alter_populated: bool = True):
    """Build a minimal DestinationConfig-like for _ensure_partition_spec tests.

    Defaults `partition_by_allow_alter_populated=True` so the bulk of tests
    don't have to think about the populated gate — they're testing the
    spec-application logic. The gate has dedicated tests.
    """
    cfg = MagicMock()
    cfg.partition_by = partition_by
    cfg.partition_by_allow_alter_populated = partition_by_allow_alter_populated
    return cfg


def _make_table(
    *,
    is_unpartitioned: bool,
    fields: tuple | None = None,
    has_data: bool = False,
):
    """Build a mock pyducklake Table with `spec` and `is_unpartitioned` as
    PROPERTIES (matching the real pyducklake API).

    The previous mock setup used `table.spec.return_value` / `.is_unpartitioned.return_value`
    which made them callable — masking the production bug where the implementation
    called them as methods (`table.spec()`) and TypeError'd at runtime. PropertyMock
    enforces the property contract so tests fail loudly if the code regresses.

    `has_data` controls what `_table_has_data` sees when it probes via
    `catalog.connection.execute(...).fetchone()` — True returns a row, False
    returns None.
    """
    from unittest.mock import PropertyMock

    table = MagicMock()
    spec_obj = MagicMock()
    # `is_unpartitioned` is a property on PartitionSpec
    type(spec_obj).is_unpartitioned = PropertyMock(return_value=is_unpartitioned)
    if fields is not None:
        spec_obj.fields = fields
    # `spec` is a property on Table
    type(table).spec = PropertyMock(return_value=spec_obj)
    # Wire the catalog.connection.execute().fetchone() chain for _table_has_data
    table.fully_qualified_name = "test.test_table"
    table.catalog.connection.execute.return_value.fetchone.return_value = (1,) if has_data else None
    return table


def test_ensure_partition_spec_noop_when_config_empty():
    """No partition_by in config → no calls into pyducklake at all (don't
    interrogate the table; saves a round-trip)."""
    from viaduck.destination import _ensure_partition_spec

    table = MagicMock()
    _ensure_partition_spec(table, _make_dest_cfg(partition_by=()), "team-2")
    # Neither the property nor update_spec should be touched
    table.update_spec.assert_not_called()


def test_ensure_partition_spec_applies_when_table_unpartitioned():
    """Config has partition_by + table is currently unpartitioned →
    UpdateSpec().add_field(...).commit() with correct transform mapping."""
    from pyducklake.partitioning import HOUR, IDENTITY, YEAR

    from viaduck.destination import _ensure_partition_spec

    table = _make_table(is_unpartitioned=True)
    update_spec = MagicMock()
    table.update_spec.return_value = update_spec

    cfg = _make_dest_cfg(
        partition_by=(
            ("", "team_id"),
            ("year", "_inserted_at"),
            ("hour", "_inserted_at"),
        )
    )
    _ensure_partition_spec(table, cfg, "team-2")

    assert update_spec.add_field.call_count == 3
    call_args = [(c.args[0], c.args[1]) for c in update_spec.add_field.call_args_list]
    assert call_args == [
        ("team_id", IDENTITY),
        ("_inserted_at", YEAR),
        ("_inserted_at", HOUR),
    ]
    update_spec.commit.assert_called_once()


def test_ensure_partition_spec_skips_already_partitioned_matching_table():
    """Config has partition_by + table is already partitioned with the SAME
    spec → silent no-op. No ALTER, no warning (matches is the happy path)."""
    from pyducklake.partitioning import IDENTITY, YEAR

    from viaduck.destination import _ensure_partition_spec

    matching_fields = (
        MagicMock(source_column="team_id", transform=IDENTITY),
        MagicMock(source_column="_inserted_at", transform=YEAR),
    )
    table = _make_table(is_unpartitioned=False, fields=matching_fields)
    cfg = _make_dest_cfg(
        partition_by=(
            ("", "team_id"),
            ("year", "_inserted_at"),
        )
    )
    _ensure_partition_spec(table, cfg, "team-2")
    table.update_spec.assert_not_called()


def test_ensure_partition_spec_warns_on_divergent_existing_spec(caplog):
    """Config has partition_by + table is already partitioned with a DIFFERENT
    spec → WARNING logged, no ALTER. Operator decides whether to reconcile."""
    import logging

    from pyducklake.partitioning import IDENTITY, MONTH

    from viaduck.destination import _ensure_partition_spec

    # Existing spec on the table is (team_id, month(_inserted_at)) but config
    # wants (team_id, year(_inserted_at)).
    diverging_fields = (
        MagicMock(source_column="team_id", transform=IDENTITY),
        MagicMock(source_column="_inserted_at", transform=MONTH),
    )
    table = _make_table(is_unpartitioned=False, fields=diverging_fields)
    cfg = _make_dest_cfg(
        partition_by=(
            ("", "team_id"),
            ("year", "_inserted_at"),
        )
    )

    with caplog.at_level(logging.WARNING, logger="viaduck.destination"):
        _ensure_partition_spec(table, cfg, "team-2")

    table.update_spec.assert_not_called()
    assert any("has partition spec" in r.message and "config specifies" in r.message for r in caplog.records)


def test_ensure_partition_spec_catch_verify_lost_race_to_peer():
    """Race scenario: we observed the table as unpartitioned, attempted ALTER,
    but a peer pod committed first. Our commit() raises; we refresh and find
    the spec applied; we verify it matches; we treat as success."""
    from unittest.mock import PropertyMock

    from pyducklake.partitioning import IDENTITY, YEAR

    from viaduck.destination import _ensure_partition_spec

    table = MagicMock()
    spec_obj = MagicMock()
    # First .spec.is_unpartitioned read: True (so we attempt the ALTER).
    # After refresh: False (a peer has applied the spec we wanted).
    # `side_effect=[True, False]` over a list (not a generator-with-lambda)
    # gives a legible StopIteration with traceback if the implementation
    # ever does a third read — keeps the test brittle in a useful way
    # rather than masking a regression.
    type(spec_obj).is_unpartitioned = PropertyMock(side_effect=[True, False])
    # spec.fields used by _verify_or_warn after refresh
    spec_obj.fields = (
        MagicMock(source_column="team_id", transform=IDENTITY),
        MagicMock(source_column="_inserted_at", transform=YEAR),
    )
    type(table).spec = PropertyMock(return_value=spec_obj)

    failing_update = MagicMock()
    failing_update.commit.side_effect = RuntimeError("peer raced us")
    table.update_spec.return_value = failing_update

    cfg = _make_dest_cfg(
        partition_by=(
            ("", "team_id"),
            ("year", "_inserted_at"),
        )
    )

    # Should NOT raise — the catch-verify path recognizes the spec is now applied
    _ensure_partition_spec(table, cfg, "team-2")

    failing_update.commit.assert_called_once()
    table.refresh.assert_called_once()


def test_ensure_partition_spec_catch_verify_genuine_failure_reraises():
    """If commit() fails AND the post-refresh spec is still unpartitioned, that's
    not a race — it's a real error (e.g., column missing). Re-raise so the pod
    startup fails loudly."""
    from unittest.mock import PropertyMock

    from viaduck.destination import _ensure_partition_spec

    table = MagicMock()
    spec_obj = MagicMock()
    # Always unpartitioned, before and after refresh — no peer rescued us
    type(spec_obj).is_unpartitioned = PropertyMock(return_value=True)
    type(table).spec = PropertyMock(return_value=spec_obj)

    failing_update = MagicMock()
    failing_update.commit.side_effect = RuntimeError("column does not exist")
    table.update_spec.return_value = failing_update

    cfg = _make_dest_cfg(partition_by=(("", "nonexistent_column"),))

    with pytest.raises(RuntimeError, match="column does not exist"):
        _ensure_partition_spec(table, cfg, "team-2")

    table.refresh.assert_called_once()


def test_ensure_partition_spec_refuses_alter_when_table_has_data_and_gate_off(caplog):
    """The populated-table safety gate: if the destination table has
    existing data AND partition_by_allow_alter_populated is False (default),
    refuse the ALTER and log an ERROR. The table is left unpartitioned and
    the pod startup continues — operator must explicitly opt in."""
    import logging

    from viaduck.destination import _ensure_partition_spec

    table = _make_table(is_unpartitioned=True, has_data=True)
    cfg = _make_dest_cfg(
        partition_by=(("", "team_id"),),
        partition_by_allow_alter_populated=False,
    )
    with caplog.at_level(logging.ERROR, logger="viaduck.destination"):
        _ensure_partition_spec(table, cfg, "team-2")

    table.update_spec.assert_not_called()
    assert any("Refusing to ALTER destination" in r.message for r in caplog.records)


def test_ensure_partition_spec_proceeds_when_table_empty_regardless_of_gate():
    """A fresh, empty destination table is always safe to ALTER — the gate
    only applies when there's actual data to risk corrupting."""
    from viaduck.destination import _ensure_partition_spec

    table = _make_table(is_unpartitioned=True, has_data=False)
    update_spec = MagicMock()
    table.update_spec.return_value = update_spec
    cfg = _make_dest_cfg(
        partition_by=(("", "team_id"),),
        partition_by_allow_alter_populated=False,  # gate is OFF
    )

    _ensure_partition_spec(table, cfg, "team-2")

    # Empty table → gate doesn't block → ALTER proceeds
    update_spec.commit.assert_called_once()


def test_ensure_partition_spec_proceeds_on_populated_when_gate_on():
    """Operator opt-in: with the gate flipped True, ALTER proceeds even
    against a populated table. This is the path that ships the actual
    posthog.events_nrt migration once we've verified ALTER behavior."""
    from viaduck.destination import _ensure_partition_spec

    table = _make_table(is_unpartitioned=True, has_data=True)
    update_spec = MagicMock()
    table.update_spec.return_value = update_spec
    cfg = _make_dest_cfg(
        partition_by=(("", "team_id"),),
        partition_by_allow_alter_populated=True,  # operator opt-in
    )

    _ensure_partition_spec(table, cfg, "team-2")
    update_spec.commit.assert_called_once()


def test_ensure_partition_spec_treats_probe_failure_as_populated(caplog):
    """If the SELECT-1 probe raises (network, ducklake transient), we
    conservatively treat the table as populated — refuse to ALTER until
    we can verify emptiness on a future cold-connect."""
    import logging

    from viaduck.destination import _ensure_partition_spec

    table = _make_table(is_unpartitioned=True, has_data=False)
    # Override execute to raise — simulates a transient catalog failure
    table.catalog.connection.execute.side_effect = RuntimeError("transient catalog read failed")
    cfg = _make_dest_cfg(
        partition_by=(("", "team_id"),),
        partition_by_allow_alter_populated=False,
    )

    with caplog.at_level(logging.WARNING, logger="viaduck.destination"):
        _ensure_partition_spec(table, cfg, "team-2")

    table.update_spec.assert_not_called()
    assert any("Failed to probe" in r.message for r in caplog.records)


def test_table_has_data_refuses_fqn_with_sql_metachar(caplog):
    """Defensive guard: if pyducklake's `fully_qualified_name` ever leaks a
    semicolon, comment marker, or newline, we refuse to interpolate it
    into the probe SELECT and treat the table as populated. This catches
    a pyducklake contract regression OR an exotic operator-controlled
    table name that smuggles SQL meta-chars."""
    import logging

    from viaduck.destination import _table_has_data

    table = MagicMock()
    table.fully_qualified_name = "evil; DROP TABLE x;--"
    with caplog.at_level(logging.ERROR, logger="viaduck.destination"):
        assert _table_has_data(table) is True
    # The catalog connection must NOT have been touched.
    table.catalog.connection.execute.assert_not_called()
    assert any("suspicious fully_qualified_name" in r.message for r in caplog.records)


def test_ensure_partition_spec_metric_counts_each_outcome():
    """Smoke test that each outcome label increments `partition_spec_total`.

    Doesn't try to verify exact label combinations across the full matrix —
    that's brittle to mock. Verifies the metric is wired into each branch
    so a regression that drops the inc() will show up as zero traffic on
    a label that should be present."""
    from viaduck import metrics
    from viaduck.destination import _ensure_partition_spec

    metrics.init("test-partition-metric")
    counter = metrics.partition_spec_total

    # skipped_no_config branch
    before = counter.labels(destination="d", outcome="skipped_no_config")._value.get()
    _ensure_partition_spec(MagicMock(), _make_dest_cfg(partition_by=()), "d")
    assert counter.labels(destination="d", outcome="skipped_no_config")._value.get() == before + 1

    # applied branch
    table = _make_table(is_unpartitioned=True, has_data=False)
    table.update_spec.return_value = MagicMock()
    before = counter.labels(destination="d", outcome="applied")._value.get()
    _ensure_partition_spec(
        table,
        _make_dest_cfg(partition_by=(("", "team_id"),), partition_by_allow_alter_populated=True),
        "d",
    )
    assert counter.labels(destination="d", outcome="applied")._value.get() == before + 1

    # refused_populated branch
    table = _make_table(is_unpartitioned=True, has_data=True)
    before = counter.labels(destination="d", outcome="refused_populated")._value.get()
    _ensure_partition_spec(
        table,
        _make_dest_cfg(partition_by=(("", "team_id"),), partition_by_allow_alter_populated=False),
        "d",
    )
    assert counter.labels(destination="d", outcome="refused_populated")._value.get() == before + 1


def test_connect_errors_scrub_credentials():
    from viaduck.scrub import scrub_credentials as _scrub_credentials

    kv = 'ATTACH failed: Unable to connect to Postgres at "host=h port=5432 user=u password=SECRETPW dbname=d"'
    assert "SECRETPW" not in _scrub_credentials(kv)
    assert "password=***" in _scrub_credentials(kv)
    quoted = "could not attach: password='SECRET PW' dbname=d"
    assert "SECRET" not in _scrub_credentials(quoted)
    url = 'IOException: Cannot open file "postgresql://user:SECRETPW@host:5432/db"'
    assert "SECRETPW" not in _scrub_credentials(url)
    assert "user:***@host" in _scrub_credentials(url)
    # SQL ''-doubled quoting (discovery.build_attach_uri): a pre-parse
    # exception embeds the raw doubled-quote literal; a backslash-only
    # pattern would terminate halfway and leak the password tail.
    doubled = "Parser Error: syntax error in \"ATTACH '... password='SEC''RET' dbname=d' ...\""
    assert "SEC" not in _scrub_credentials(doubled).split("password=***")[1]
    assert "password=***" in _scrub_credentials(doubled)
    assert "RET" not in _scrub_credentials(doubled)


def test_scrub_hardened_formats():
    from viaduck.scrub import scrub_credentials as _scrub_credentials

    for text in (
        "password = SECRETPW dbname=d",
        "PASSWORD=SECRETPW host=h",
        "{'password': 'SECRETPW', 'host': 'h'}",
        'IOException: "postgresql://user:SEC@RET@PW@host:5432/db"',
    ):
        scrubbed = _scrub_credentials(text)
        assert "SECRETPW" not in scrubbed and "SEC@RET" not in scrubbed, (text, scrubbed)
    # Greedy userinfo scrub keeps the host visible.
    assert "@host:5432" in _scrub_credentials('x "postgresql://user:SEC@RET@host:5432/db"')


def test_connect_error_suppresses_original_via_log(caplog):
    import logging as _logging

    from viaduck.destination import DestinationConnectError
    from viaduck.scrub import scrub_credentials as _scrub_credentials

    logger = _logging.getLogger("test.scrub")
    try:
        try:
            raise RuntimeError("attach failed: password=SUPERSECRET host=h")
        except RuntimeError as e:
            raise DestinationConnectError(f"destination d1: {_scrub_credentials(str(e))}") from None
    except DestinationConnectError:
        with caplog.at_level(_logging.ERROR, logger="test.scrub"):
            logger.exception("Flush failed for destination d1")
    text = caplog.text
    assert "SUPERSECRET" not in text
    assert "password=***" in text


def test_scrub_is_linear_on_adversarial_input():
    # CodeQL py/redos: the earlier quoted-value pattern backtracked
    # exponentially on an unclosed quote followed by many escape pairs.
    # The unrolled-loop form must stay linear.
    import time as _time

    from viaduck.scrub import scrub_credentials

    hostile = "password='" + "\\&" * 20000  # unclosed quote, 20k escape pairs
    t0 = _time.monotonic()
    scrub_credentials(hostile)
    scrub_credentials('{"password":"' + "\\!" * 20000)
    assert _time.monotonic() - t0 < 1.0


# --- Registry indirection (C3: stale-capture class) ---


def test_create_resolves_via_registry_never_frozen_config():
    """The stale-capture pin: _create resolves the destination through the
    LIVE registry, never through the frozen startup config. Three separate
    stale-captured-config defects motivated this — a pool holding a
    pre-merge config made every discovered destination buffer forever."""
    from viaduck.config import DestinationConfig
    from viaduck.registry import DestinationRegistry

    config = MagicMock()  # the frozen startup config: must never be asked
    registry = DestinationRegistry()
    registry.add(
        DestinationConfig(
            id="dyn-1",
            routing_value="acme",
            name="cat-dyn-1",
            postgres_uri_env="UNUSED",
            data_path="s3://bucket/dyn-1",
            table="events",
            postgres_uri_direct="postgresql://u:p@h/db",
        ),
        origin="discovered",
    )
    pool = DestinationPool(config, registry, max_open=3)
    pool.set_source_schema(MagicMock())

    fake_catalog = MagicMock()
    with patch("pyducklake.Catalog", return_value=fake_catalog):
        catalog, table, _plan = pool._create("dyn-1")

    assert catalog is fake_catalog
    config.destination_by_id.assert_not_called()


# --- Deferred credential resolution (C3 §5 secret-ref deferral) ---


class TestDeferredResolution:
    def teardown_method(self):
        # Real-cache tests must not leak entries into later files
        # (verification finding 2: latent order-coupling).
        from viaduck import k8s_secrets

        k8s_secrets._cache_clear()

    def _deferred_cfg(self):
        from viaduck.config import DeferredUriSource, DestinationConfig

        return DestinationConfig(
            id="dyn-1",
            routing_value="acme",
            name="cat-dyn-1",
            postgres_uri_env="",
            data_path="s3://bucket/dyn-1",
            table="events",
            uri_source=DeferredUriSource(
                pg_endpoint="pooler.cnpg-shards.svc.cluster.local",
                pg_port=5432,
                pg_database="acme",
                pg_username="acme_user",
                secret_namespace="ducklings",
                secret_name="cnpg-tenant-acme-password",
                secret_key="password",
                sslmode="require",
            ),
        )

    def _pool_with(self, cfg_obj):
        from viaduck.registry import DestinationRegistry

        registry = DestinationRegistry()
        registry.add(cfg_obj, origin="discovered")
        config = MagicMock()
        config.discovery.secret_cache_ttl_s = 300.0
        config.discovery.request_timeout_s = 10.0
        pool = DestinationPool(config, registry, max_open=3)
        pool.set_source_schema(MagicMock())
        return pool

    def test_create_resolves_ref_and_builds_uri_per_connect(self):
        pool = self._pool_with(self._deferred_cfg())
        fake_catalog = MagicMock()
        with (
            patch("viaduck.k8s_secrets.read_secret_key_cached", return_value="p'w") as rd,
            patch("pyducklake.Catalog", return_value=fake_catalog) as cat,
        ):
            pool._create("dyn-1")
        rd.assert_called_once_with("ducklings", "cnpg-tenant-acme-password", "password", ttl_s=300.0, timeout_s=10.0)
        uri = cat.call_args.args[1]
        # The two stacked parse layers (libpq keyword form + SQL-doubled
        # quotes) apply at connect time now.
        assert uri.startswith("postgres:host=")
        assert "password=''p\\''w''" in uri
        assert "sslmode=''require''" in uri

    def test_secret_failure_becomes_scrubbed_connect_error(self):
        from viaduck.destination import DestinationConnectError
        from viaduck.k8s_secrets import SecretReadError

        pool = self._pool_with(self._deferred_cfg())
        with (
            patch(
                "viaduck.k8s_secrets.read_secret_key_cached",
                side_effect=SecretReadError("RBAC denied"),
            ),
            pytest.raises(DestinationConnectError, match="dyn-1"),
        ):
            pool._create("dyn-1")

    def test_static_config_path_unchanged(self):
        from viaduck.config import DestinationConfig
        from viaduck.registry import DestinationRegistry

        static = DestinationConfig(
            id="s1",
            routing_value="2",
            name="cat-s1",
            postgres_uri_env="UNUSED",
            data_path="s3://bucket/s1",
            table="events",
            postgres_uri_direct="postgres:host=rds",
        )
        registry = DestinationRegistry()
        registry.add(static, origin="static")
        pool = DestinationPool(MagicMock(), registry, max_open=3)
        pool.set_source_schema(MagicMock())
        with (
            patch("viaduck.k8s_secrets.read_secret_key_cached") as rd,
            patch("pyducklake.Catalog", return_value=MagicMock()) as cat,
        ):
            pool._create("s1")
        rd.assert_not_called()
        assert cat.call_args.args[1] == "postgres:host=rds"

    def test_probe_warms_cache_so_first_connect_does_no_api_read(self):
        # The materialize->_create seam: the probe's cached read means the
        # pool's first connect resolves from the cache — exactly one API
        # call end-to-end. Patches the INNER reader so the real cache
        # engages across both calls.
        from viaduck import discovery, k8s_secrets

        k8s_secrets._cache_clear()
        payload = {
            "config_generation": 1,
            "warehouses": [
                {
                    "org_id": "acme",
                    "writable": True,
                    "state": "ready",
                    "bucket": "b",
                    "metadata_store": {
                        "endpoint": "pooler.cnpg-shards.svc.cluster.local",
                        "database": "acme",
                        "username": "acme_user",
                        "password_secret_ref": {"namespace": "ducklings", "name": "s", "key": "password"},
                    },
                    "teams": [{"team_id": 7, "events_table": "t.events"}],
                }
            ],
        }
        with patch("viaduck.k8s_secrets.read_secret_key", return_value="pw") as rd:
            mapped = discovery.map_payload(payload)
            configs = discovery.materialize(mapped, set(), {})
            assert len(configs) == 1
            pool = self._pool_with(configs[0])
            with patch("pyducklake.Catalog", return_value=MagicMock()):
                pool._create(configs[0].id)
        rd.assert_called_once()

    def test_connect_failure_invalidates_cached_secret(self):
        from viaduck import k8s_secrets
        from viaduck.destination import DestinationConnectError

        k8s_secrets._cache_clear()
        pool = self._pool_with(self._deferred_cfg())
        with patch("viaduck.k8s_secrets.read_secret_key", side_effect=["rotated-away", "fresh"]) as rd:
            with (
                patch("pyducklake.Catalog", side_effect=RuntimeError("password authentication failed")),
                pytest.raises(DestinationConnectError),
            ):
                pool._create("dyn-1")
            # Next attempt re-reads the API (cache invalidated), not the
            # stale entry — rotation heals in one flush cycle.
            with patch("pyducklake.Catalog", return_value=MagicMock()):
                pool._create("dyn-1")
        assert rd.call_count == 2
