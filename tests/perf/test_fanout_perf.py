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
from viaduck.config import RoutingConfig
from viaduck.main import _build_delete_filter, _resolve_conflicts, _resolve_preimages
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
    cfg = RoutingConfig(field="company")
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
    cfg = RoutingConfig(field="company")
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
    cfg = RoutingConfig(field="company")
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
