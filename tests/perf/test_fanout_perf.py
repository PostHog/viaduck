"""Performance tests for viaduck fanout path.

Exercises Router.split_and_count, _resolve_preimages, _resolve_conflicts,
and _build_delete_filter under larger-than-unit-test data volumes.

Run with: just test-perf
JSON output: just test-perf-json

Limitations (acknowledged, future work):
- Single iteration per test, no warmup, no statistical analysis. Time budgets
  are calibrated for developer laptops; CI runners may need looser thresholds.
- Data is uniform (sequential IDs, even distribution). Production data has
  skew, hot keys, and variable-length strings.
- Tests measure Python processing only, not DuckDB I/O (CDC reads, writes).
- No memory profiling. to_pylist() materialization is O(N) but unmeasured.
"""

from __future__ import annotations

import random

import pyarrow as pa
import pytest

from viaduck import metrics
from viaduck.apply import _build_delete_filter, _resolve_conflicts
from viaduck.config import RoutingConfig
from viaduck.main import _resolve_preimages
from viaduck.router import Router


def setup_module():
    metrics.init("perf")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

CHANGE_TYPES = ["insert", "update_preimage", "update_postimage", "delete"]


def _make_routing_table(num_rows: int, num_destinations: int, routing_field: str = "company") -> pa.Table:
    """Build an Arrow table with deterministic routing values."""
    rng = random.Random(42)
    routing_values = [f"dest_{i}" for i in range(num_destinations)]
    companies = [routing_values[rng.randint(0, num_destinations - 1)] for _ in range(num_rows)]
    return pa.table(
        {
            routing_field: companies,
            "value": list(range(num_rows)),
        }
    )


def _make_cdc_batch(
    num_rows: int,
    change_type_weights: dict[str, float] | None = None,
    routing_field: str = "company",
    num_routing_values: int = 10,
) -> pa.Table:
    """Build a CDC batch with deterministic change_type distribution."""
    rng = random.Random(42)
    if change_type_weights is None:
        change_type_weights = {"insert": 0.4, "update_preimage": 0.15, "update_postimage": 0.15, "delete": 0.3}

    types_list = list(change_type_weights.keys())
    weights = list(change_type_weights.values())

    change_types = rng.choices(types_list, weights=weights, k=num_rows)

    routing_values = [f"tenant_{i}" for i in range(num_routing_values)]
    companies = [routing_values[rng.randint(0, num_routing_values - 1)] for _ in range(num_rows)]

    # For preimages, pair with postimages using same rowid
    rowids = list(range(num_rows))

    return pa.table(
        {
            routing_field: companies,
            "value": list(range(num_rows)),
            "change_type": change_types,
            "snapshot_id": [1] * num_rows,
            "rowid": rowids,
        }
    )


def _make_conflict_batch(num_rows: int, conflict_pct: float = 0.05) -> pa.Table:
    """Build a CDC batch where ~conflict_pct of rows are insert+delete on the same rowid."""
    rng = random.Random(42)
    num_conflicts = int(num_rows * conflict_pct / 2)  # each conflict is 2 rows
    num_plain = num_rows - (num_conflicts * 2)

    change_types: list[str] = []
    rowids: list[int] = []

    # Plain inserts
    for i in range(num_plain):
        change_types.append("insert")
        rowids.append(i)

    # Conflicting insert+delete pairs
    for i in range(num_conflicts):
        rid = num_plain + i
        change_types.append("insert")
        rowids.append(rid)
        change_types.append("delete")
        rowids.append(rid)

    # Shuffle deterministically
    combined = list(zip(change_types, rowids))
    rng.shuffle(combined)
    change_types, rowids = zip(*combined) if combined else ([], [])

    actual_rows = len(change_types)
    return pa.table(
        {
            "company": [f"tenant_{i % 10}" for i in range(actual_rows)],
            "value": list(range(actual_rows)),
            "change_type": list(change_types),
            "snapshot_id": [1] * actual_rows,
            "rowid": list(rowids),
        }
    )


# ---------------------------------------------------------------------------
# Router performance
# ---------------------------------------------------------------------------


@pytest.mark.perf
def test_router_split_100_destinations(perf_timer):
    """10K rows, 100 routing values."""
    cfg = RoutingConfig(field="company", mode="append_only")
    router = Router(cfg)
    table = _make_routing_table(10_000, 100)
    routing_values = [f"dest_{i}" for i in range(100)]

    with perf_timer("router_split", "10K rows, 100 dests") as t:
        routed, unrouted = router.split_and_count(table, routing_values)

    print(f"router split_and_count (10K rows, 100 dests): {t.elapsed:.3f}s")
    assert t.elapsed < 1.0, f"Took {t.elapsed:.3f}s, expected < 1s"
    assert sum(tbl.num_rows for tbl in routed.values()) + unrouted == 10_000


@pytest.mark.perf
def test_router_split_1000_destinations(perf_timer):
    """100K rows, 1000 routing values."""
    cfg = RoutingConfig(field="company", mode="append_only")
    router = Router(cfg)
    table = _make_routing_table(100_000, 1000)
    routing_values = [f"dest_{i}" for i in range(1000)]

    with perf_timer("router_split", "100K rows, 1000 dests") as t:
        routed, unrouted = router.split_and_count(table, routing_values)

    print(f"router split_and_count (100K rows, 1000 dests): {t.elapsed:.3f}s")
    assert t.elapsed < 5.0, f"Took {t.elapsed:.3f}s, expected < 5s"
    assert sum(tbl.num_rows for tbl in routed.values()) + unrouted == 100_000


@pytest.mark.perf
def test_router_split_1m_rows_10k_destinations(perf_timer):
    """1M rows, 10K routing values (production scale)."""
    cfg = RoutingConfig(field="company", mode="append_only")
    router = Router(cfg)
    table = _make_routing_table(1_000_000, 10_000)
    routing_values = [f"dest_{i}" for i in range(10_000)]

    with perf_timer("router_split", "1M rows, 10K dests") as t:
        routed, unrouted = router.split_and_count(table, routing_values)

    print(f"router split_and_count (1M rows, 10K dests): {t.elapsed:.3f}s")
    assert t.elapsed < 60.0, f"Took {t.elapsed:.3f}s, expected < 60s"
    assert sum(tbl.num_rows for tbl in routed.values()) + unrouted == 1_000_000


# ---------------------------------------------------------------------------
# _resolve_preimages performance
# ---------------------------------------------------------------------------


@pytest.mark.perf
def test_resolve_preimages_large_batch(perf_timer):
    """50K rows with mixed change types."""
    batch = _make_cdc_batch(50_000)

    with perf_timer("resolve_preimages", "50K rows") as t:
        result = _resolve_preimages(batch, "company", ["value"])

    print(f"_resolve_preimages (50K rows): {t.elapsed:.3f}s")
    assert t.elapsed < 2.0, f"Took {t.elapsed:.3f}s, expected < 2s"
    assert "update_preimage" not in result.column("change_type").to_pylist()


@pytest.mark.perf
def test_resolve_preimages_matched_pairs(perf_timer):
    """50K rows of MATCHED update pre/postimage pairs — exercises the
    join-probe-hit and same-tenant-drop paths, which the mixed batch
    (unique rowids, all orphans) never reaches."""
    n_pairs = 25_000
    rowids = list(range(n_pairs)) * 2
    change_types = ["update_preimage"] * n_pairs + ["update_postimage"] * n_pairs
    companies = [f"dest_{i % 50}" for i in range(n_pairs)] * 2
    batch = pa.table(
        {
            "change_type": pa.array(change_types, type=pa.string()),
            "rowid": pa.array(rowids, type=pa.int64()),
            "company": pa.array(companies, type=pa.string()),
            "value": pa.array(list(range(n_pairs)) * 2, type=pa.int64()),
        }
    )

    with perf_timer("resolve_preimages_matched", "50K rows, all matched pairs") as t:
        result = _resolve_preimages(batch, "company", ["value"])

    print(f"_resolve_preimages (50K rows, matched pairs): {t.elapsed:.3f}s")
    assert t.elapsed < 2.0, f"Took {t.elapsed:.3f}s, expected < 2s"
    # All preimages dropped (same-tenant), all postimages kept.
    assert result.num_rows == n_pairs


# ---------------------------------------------------------------------------
# _resolve_conflicts performance
# ---------------------------------------------------------------------------


@pytest.mark.perf
def test_resolve_conflicts_large_batch(perf_timer):
    """50K rows with ~5% insert+delete conflicts."""
    batch = _make_conflict_batch(50_000, conflict_pct=0.05)

    with perf_timer("resolve_conflicts", "50K rows, ~5% conflicts") as t:
        result = _resolve_conflicts(batch)

    print(f"_resolve_conflicts (50K rows, ~5% conflicts): {t.elapsed:.3f}s")
    assert t.elapsed < 1.0, f"Took {t.elapsed:.3f}s, expected < 1s"
    ct = result.column("change_type").to_pylist()
    rid = result.column("rowid").to_pylist()
    insert_rids = {rid[i] for i in range(len(ct)) if ct[i] == "insert"}
    delete_rids = {rid[i] for i in range(len(ct)) if ct[i] == "delete"}
    assert not (insert_rids & delete_rids), "Unresolved conflicts remain"


# ---------------------------------------------------------------------------
# _build_delete_filter performance
# Input tables have no metadata columns (change_type/snapshot_id/rowid) because
# _apply_changes calls strip_meta() before _build_delete_filter, matching
# production behavior.
# ---------------------------------------------------------------------------


@pytest.mark.perf
def test_build_delete_filter_large(perf_timer):
    """1000 rows, single-column key."""
    rows = pa.table({"id": list(range(1000))})

    with perf_timer("delete_filter_single_key", "1000 rows") as t:
        sql = _build_delete_filter(rows, ["id"])

    print(f"_build_delete_filter (1000 rows, single key): {t.elapsed:.3f}s")
    assert t.elapsed < 1.0, f"Took {t.elapsed:.3f}s, expected < 1s"
    assert isinstance(sql, str) and len(sql) > 0
    assert "IN" in sql


@pytest.mark.perf
def test_build_delete_filter_composite_key_large(perf_timer):
    """500 rows, 3-column composite key."""
    rows = pa.table(
        {
            "k1": list(range(500)),
            "k2": [f"val_{i}" for i in range(500)],
            "k3": list(range(500, 1000)),
        }
    )

    with perf_timer("delete_filter_composite_key", "500 rows, 3-col key") as t:
        sql = _build_delete_filter(rows, ["k1", "k2", "k3"])

    print(f"_build_delete_filter (500 rows, 3-col composite key): {t.elapsed:.3f}s")
    assert t.elapsed < 2.0, f"Took {t.elapsed:.3f}s, expected < 2s"
    assert isinstance(sql, str) and len(sql) > 0
    assert "AND" in sql
    assert "OR" in sql


# ---------------------------------------------------------------------------
# End-to-end delivery fanout: DeliveryManager + worker pool + real local
# DuckLake destination catalogs (DuckDB files, no Postgres). Measures the
# full buffer -> flush -> Phase 2/3 apply path at high destination counts.
#
# Limitations: cursor persistence is mocked (no Postgres in the perf env),
# so PG round-trips per flush are excluded; destination catalogs are local
# DuckDB files, so object-store latency is excluded. Both costs are per
# flush, not per destination-count, so the SCALING shape is honest even
# though absolute numbers flatter production.
# ---------------------------------------------------------------------------


def _make_fanout_pool(tmp_path, n_dests: int, max_open: int):
    import os
    from unittest.mock import MagicMock

    from viaduck.destination import DestinationPool
    from viaduck.registry import DestinationRegistry

    dests = {}
    for i in range(n_dests):
        dest_id = f"d{i}"
        base = tmp_path / dest_id
        os.makedirs(base / "data", exist_ok=True)
        d = MagicMock()
        d.id = dest_id
        d.name = dest_id
        d.postgres_uri = str(base / "meta.duckdb")
        d.data_path = str(base / "data")
        d.table = "events"
        d.resolved_properties.return_value = {}
        # A real DestinationConfig defaults uri_source=None; on a MagicMock
        # the auto-attr is truthy and _resolve_uri would take the
        # deferred-secret path into the k8s machinery.
        d.uri_source = None
        dests[dest_id] = d

    cfg = MagicMock()
    registry = DestinationRegistry.from_configs(list(dests.values()))
    pool = DestinationPool(cfg, registry, max_open=max_open)

    from pyducklake import Schema
    from pyducklake.types import IntegerType, NestedField, StringType

    pool.set_source_schema(
        Schema(
            NestedField(field_id=1, name="event_id", field_type=IntegerType(), required=True),
            NestedField(field_id=2, name="company", field_type=StringType(), required=True),
            NestedField(field_id=3, name="value", field_type=IntegerType()),
        )
    )
    return pool


def _fanout_cdc_batch(dest_id: str, rows_per_dest: int) -> pa.Table:
    return pa.table(
        {
            "event_id": pa.array(list(range(rows_per_dest)), type=pa.int32()),
            "company": pa.array([dest_id] * rows_per_dest, type=pa.string()),
            "value": pa.array(list(range(rows_per_dest)), type=pa.int32()),
            "change_type": pa.array(["insert"] * rows_per_dest, type=pa.string()),
            "snapshot_id": pa.array([1] * rows_per_dest, type=pa.int64()),
            "rowid": pa.array(list(range(rows_per_dest)), type=pa.int64()),
        }
    )


@pytest.mark.perf
@pytest.mark.parametrize("n_dests", [200, 500, 1000])
def test_delivery_fanout(perf_timer, tmp_path, n_dests):
    """Full-CDC flush of 100 rows to each of N destinations through the
    DeliveryManager worker pool (workers=8, pool max_open=100 — the
    production default, so LRU eviction is exercised above 100 dests)."""
    from unittest.mock import MagicMock

    from viaduck.config import DeliveryConfig
    from viaduck.delivery import DeliveryManager

    rows_per_dest = 100
    dest_ids = [f"d{i}" for i in range(n_dests)]
    pool = _make_fanout_pool(tmp_path, n_dests, max_open=100)

    state = MagicMock()
    state.load_cursors.return_value = {}

    mgr = DeliveryManager(
        DeliveryConfig(workers=8, flush_interval_seconds=0.0),
        state,
        pool,
        ["event_id"],
        dest_ids,
        mode="full_cdc",
    )

    for d in dest_ids:
        mgr.buffer(d, _fanout_cdc_batch(d, rows_per_dest), through_snapshot=1)

    with perf_timer("delivery_fanout", f"{n_dests} dests, {rows_per_dest} rows each") as t:
        submitted = mgr.maybe_flush()
        assert submitted == n_dests
        assert mgr.wait_idle(timeout_s=600)

    assert t.elapsed < 600, f"Took {t.elapsed:.1f}s, expected < 600s"
    statuses = mgr.status_snapshot()
    failed = [d for d in dest_ids if statuses[d].last_error is not None]
    assert not failed, f"{len(failed)} destinations failed: {failed[:5]}"
    assert all(statuses[d].flushed_snapshot == 1 for d in dest_ids)
    # Every row actually applied — a flush that silently wrote nothing
    # would otherwise pass on timing alone.
    assert all(statuses[d].rows_replicated == rows_per_dest for d in dest_ids)

    total_rows = n_dests * rows_per_dest
    print(
        f"delivery fanout ({n_dests} dests, {total_rows} rows): {t.elapsed:.2f}s "
        f"= {n_dests / t.elapsed:.0f} dests/s, {total_rows / t.elapsed:.0f} rows/s"
    )
    mgr.drain(timeout_s=30)
    pool.close_all()
