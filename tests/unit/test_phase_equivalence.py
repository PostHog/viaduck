"""Equivalence locks: vectorized CDC phases vs the row-loop predecessors.

The Arrow-native implementations of _resolve_preimages, _resolve_conflicts,
and Router.split_and_count replaced per-row Python loops. These tests pin
exact behavioral equivalence by running randomized CDC batches through both
the new implementations and verbatim copies of the predecessors (the
oracles below), asserting identical outputs INCLUDING row order.

The oracle functions are frozen copies of the pre-vectorization code
(metrics/logging stripped). Do not "improve" them — their value is being
the old behavior, bug-for-bug.
"""

from __future__ import annotations

import random

import pyarrow as pa
import pytest

from viaduck import metrics
from viaduck.config import RoutingConfig
from viaduck.main import _resolve_conflicts, _resolve_preimages
from viaduck.router import Router


def setup_module():
    metrics.init("test")


# ---------------------------------------------------------------------------
# Oracles — verbatim pre-vectorization logic (metrics/logging removed)
# ---------------------------------------------------------------------------


def _oracle_resolve_preimages(batch: pa.Table, routing_field: str) -> pa.Table:
    ct_col = batch.column("change_type")
    if "update_preimage" not in ct_col.to_pylist():
        return batch

    ct_list = ct_col.to_pylist()
    routing_list = batch.column(routing_field).to_pylist()
    rowid_list = batch.column("rowid").to_pylist()

    postimage_routing: dict = {}
    for i in range(batch.num_rows):
        if ct_list[i] == "update_postimage":
            postimage_routing[rowid_list[i]] = routing_list[i]

    keep_mask = []
    new_change_types = list(ct_list)
    _SENTINEL = object()

    for i in range(batch.num_rows):
        ct = ct_list[i]
        if ct != "update_preimage":
            keep_mask.append(True)
            continue
        rowid = rowid_list[i]
        pre_routing = routing_list[i]
        post_routing = postimage_routing.get(rowid, _SENTINEL)
        if post_routing is _SENTINEL:
            new_change_types[i] = "delete"
            keep_mask.append(True)
        elif pre_routing != post_routing:
            new_change_types[i] = "delete"
            keep_mask.append(True)
        else:
            keep_mask.append(False)

    idx = batch.column_names.index("change_type")
    batch = batch.set_column(idx, "change_type", pa.array(new_change_types, type=pa.string()))
    if all(keep_mask):
        return batch
    return batch.filter(pa.array(keep_mask))


def _oracle_resolve_conflicts(batch: pa.Table) -> pa.Table:
    if batch.num_rows == 0:
        return batch

    ct_list = batch.column("change_type").to_pylist()
    rowid_list = batch.column("rowid").to_pylist()

    delete_rowids: dict = {}
    insert_rowids: dict = {}
    postimage_rowids: dict = {}

    for i in range(batch.num_rows):
        ct = ct_list[i]
        rowid = rowid_list[i]
        if ct == "delete":
            delete_rowids.setdefault(rowid, []).append(i)
        elif ct == "insert":
            insert_rowids.setdefault(rowid, []).append(i)
        elif ct == "update_postimage":
            postimage_rowids.setdefault(rowid, []).append(i)

    rows_to_remove: set = set()
    for rowid, ins_indices in insert_rowids.items():
        if rowid in postimage_rowids and rowid not in delete_rowids:
            rows_to_remove.update(ins_indices)
    for rowid, del_indices in delete_rowids.items():
        if rowid in insert_rowids:
            rows_to_remove.update(del_indices)
            rows_to_remove.update(insert_rowids[rowid])
        if rowid in postimage_rowids:
            rows_to_remove.update(postimage_rowids[rowid])

    if not rows_to_remove:
        return batch
    keep_mask = [i not in rows_to_remove for i in range(batch.num_rows)]
    return batch.filter(pa.array(keep_mask))


def _oracle_split_and_count(router: Router, table: pa.Table, routing_values: list[str]):
    import pyarrow.compute as pc

    result: dict[str, pa.Table] = {}
    column = table.column(router.field)
    total_routed = 0
    for val in routing_values:
        scalar = router._make_scalar(val, column.type)
        mask = pc.equal(column, scalar)
        filtered = table.filter(mask)
        if filtered.num_rows > 0:
            result[val] = filtered
            total_routed += filtered.num_rows
    return result, table.num_rows - total_routed


# ---------------------------------------------------------------------------
# Randomized batch generator
# ---------------------------------------------------------------------------

ROUTING_VALUES = ["a", "b", "c"]


def _random_batch(rng: random.Random, n_rows: int, *, with_mutations: bool, with_nulls: bool) -> pa.Table:
    """A CDC-shaped batch exercising every classification path: matched
    same-tenant updates, cross-tenant mutations, orphaned preimages,
    insert/delete/postimage conflicts on shared rowids, and (optionally)
    null routing values."""
    change_types, rowids, routings, vals = [], [], [], []

    def _routing():
        if with_nulls and rng.random() < 0.1:
            return None
        return rng.choice(ROUTING_VALUES + (["zz-unrouted"] if rng.random() < 0.2 else []))

    rowid = 0
    while len(change_types) < n_rows:
        rowid += 1
        shape = rng.random()
        rv = _routing()
        if shape < 0.35:  # plain insert (maybe later conflicting events, same rowid)
            change_types.append("insert")
            rowids.append(rowid)
            routings.append(rv)
            if rng.random() < 0.4:  # follow-up event for the same rowid
                follow = rng.choice(["delete", "update_postimage"])
                change_types.append(follow)
                rowids.append(rowid)
                routings.append(rv)
        elif shape < 0.6:  # update pair (pre+post), possibly mutated routing
            post_rv = rv
            if with_mutations and rng.random() < 0.3:
                post_rv = rng.choice([v for v in ROUTING_VALUES if v != rv]) if rv is not None else "a"
            change_types.append("update_preimage")
            rowids.append(rowid)
            routings.append(rv)
            change_types.append("update_postimage")
            rowids.append(rowid)
            routings.append(post_rv)
            if rng.random() < 0.2:  # delete after the update, same rowid
                change_types.append("delete")
                rowids.append(rowid)
                routings.append(post_rv)
        elif shape < 0.75:  # orphaned preimage
            change_types.append("update_preimage")
            rowids.append(rowid)
            routings.append(rv)
        else:  # plain delete
            change_types.append("delete")
            rowids.append(rowid)
            routings.append(rv)

    n = len(change_types)
    vals = [rng.randint(0, 5) for _ in range(n)]
    # Shuffle row order — CDC batches are unordered sets.
    order = list(range(n))
    rng.shuffle(order)
    return pa.table(
        {
            "change_type": pa.array([change_types[i] for i in order], type=pa.string()),
            "rowid": pa.array([rowids[i] for i in order], type=pa.int64()),
            "company": pa.array([routings[i] for i in order], type=pa.string()),
            "snapshot_id": pa.array([rng.randint(1, 50) for _ in range(n)], type=pa.int64()),
            "val": pa.array([vals[i] for i in order], type=pa.int64()),
        }
    )


def _assert_tables_equal(actual: pa.Table, expected: pa.Table, label: str) -> None:
    assert actual.num_rows == expected.num_rows, f"{label}: row count {actual.num_rows} != oracle {expected.num_rows}"
    assert actual.column_names == expected.column_names
    # Exact equality including row order.
    assert actual.combine_chunks().equals(expected.combine_chunks()), (
        f"{label}: content/order mismatch\nactual:\n{actual.to_pydict()}\noracle:\n{expected.to_pydict()}"
    )


# ---------------------------------------------------------------------------
# Equivalence tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("seed", range(20))
@pytest.mark.parametrize("with_nulls", [False, True])
def test_resolve_preimages_equivalence(seed, with_nulls):
    rng = random.Random(seed)
    batch = _random_batch(rng, 200, with_mutations=True, with_nulls=with_nulls)
    actual = _resolve_preimages(batch, "company", ["val"])
    expected = _oracle_resolve_preimages(batch, "company")
    _assert_tables_equal(actual, expected, f"preimages seed={seed} nulls={with_nulls}")


@pytest.mark.parametrize("seed", range(20))
def test_resolve_conflicts_equivalence(seed):
    rng = random.Random(seed + 1000)
    batch = _random_batch(rng, 200, with_mutations=False, with_nulls=False)
    # Phase 2 runs after Phase 1 in the pipeline; feed it resolved batches.
    resolved = _oracle_resolve_preimages(batch, "company")
    actual = _resolve_conflicts(resolved)
    expected = _oracle_resolve_conflicts(resolved)
    _assert_tables_equal(actual, expected, f"conflicts seed={seed}")


@pytest.mark.parametrize("seed", range(20))
@pytest.mark.parametrize("with_nulls", [False, True])
def test_split_and_count_equivalence(seed, with_nulls):
    rng = random.Random(seed + 2000)
    batch = _random_batch(rng, 200, with_mutations=True, with_nulls=with_nulls)
    router = Router(RoutingConfig(field="company", key_columns=["val"], seed_mode="scan"))
    actual_routed, actual_unrouted = router.split_and_count(batch, ROUTING_VALUES)
    expected_routed, expected_unrouted = _oracle_split_and_count(router, batch, ROUTING_VALUES)
    assert actual_unrouted == expected_unrouted
    assert set(actual_routed) == set(expected_routed)
    for val in expected_routed:
        _assert_tables_equal(actual_routed[val], expected_routed[val], f"router[{val}] seed={seed}")


def test_resolve_preimages_empty_and_no_preimage_passthrough():
    empty = pa.table(
        {
            "change_type": pa.array([], type=pa.string()),
            "rowid": pa.array([], type=pa.int64()),
            "company": pa.array([], type=pa.string()),
        }
    )
    assert _resolve_preimages(empty, "company", []).num_rows == 0
    plain = pa.table({"change_type": ["insert"], "rowid": [1], "company": ["a"]})
    assert _resolve_preimages(plain, "company", []) is plain  # untouched fast path


def test_resolve_conflicts_empty_passthrough():
    empty = pa.table({"change_type": pa.array([], type=pa.string()), "rowid": pa.array([], type=pa.int64())})
    assert _resolve_conflicts(empty).num_rows == 0


def test_resolve_preimages_all_orphans_no_postimages():
    """Zero postimages in the batch — the join's build side is empty.
    Regression lock: an empty pa.array([]) infers type null, which acero
    joins reject for non-key fields; the marker column must be typed."""
    batch = pa.table(
        {
            "change_type": pa.array(["update_preimage", "insert", "update_preimage"], type=pa.string()),
            "rowid": pa.array([1, 2, 3], type=pa.int64()),
            "company": pa.array(["a", "b", None], type=pa.string()),
        }
    )
    actual = _resolve_preimages(batch, "company", [])
    expected = _oracle_resolve_preimages(batch, "company")
    _assert_tables_equal(actual, expected, "all-orphans")


# ---------------------------------------------------------------------------
# Contract-edge equivalence (M1 review findings)
# ---------------------------------------------------------------------------


def _table(change_types, rowids, routings, **extra):
    cols = {
        "change_type": pa.array(change_types, type=pa.string()),
        "rowid": pa.array(rowids, type=pa.int64()),
        "company": pa.array(routings, type=pa.string()),
    }
    cols.update(extra)
    return pa.table(cols)


def test_duplicate_postimages_last_wins():
    """Out-of-contract: two postimages for one rowid. The predecessor's dict
    build made the LAST row win for preimage classification; the vectorized
    max-index dedup must match."""
    batch = _table(
        ["update_preimage", "update_postimage", "update_postimage"],
        [7, 7, 7],
        ["a", "b", "a"],  # last postimage routing == preimage routing -> drop preimage
    )
    actual = _resolve_preimages(batch, "company", [])
    expected = _oracle_resolve_preimages(batch, "company")
    _assert_tables_equal(actual, expected, "dup-postimage-last-wins")
    # And the reverse order: last postimage differs -> preimage becomes delete
    batch2 = _table(
        ["update_preimage", "update_postimage", "update_postimage"],
        [7, 7, 7],
        ["a", "a", "b"],
    )
    _assert_tables_equal(
        _resolve_preimages(batch2, "company", []),
        _oracle_resolve_preimages(batch2, "company"),
        "dup-postimage-last-wins-mutated",
    )


def test_duplicate_preimages_and_multiple_update_pairs():
    batch = _table(
        ["update_preimage", "update_preimage", "update_postimage", "insert", "update_preimage", "update_postimage"],
        [3, 3, 3, 4, 4, 4],
        ["a", "a", "a", "b", "b", "b"],
    )
    _assert_tables_equal(
        _resolve_preimages(batch, "company", []),
        _oracle_resolve_preimages(batch, "company"),
        "dup-preimages-multi-pairs",
    )


def test_mutation_direction_null_asymmetry():
    """pre=None/post='a' and pre='a'/post=None must classify exactly like the
    predecessor's Python != (both are mutations)."""
    batch = _table(
        ["update_preimage", "update_postimage", "update_preimage", "update_postimage"],
        [1, 1, 2, 2],
        [None, "a", "a", None],
    )
    _assert_tables_equal(
        _resolve_preimages(batch, "company", []),
        _oracle_resolve_preimages(batch, "company"),
        "null-mutation-directions",
    )


@pytest.mark.parametrize(
    "column,values",
    [
        (pa.array([1, 2, 3, 2, None], type=pa.int64()), ["1", "2"]),
        (pa.array([True, False, True, None, False], type=pa.bool_()), ["true", "no"]),
    ],
)
def test_split_and_count_typed_columns_equivalence(column, values):
    table = pa.table({"company": column, "v": pa.array(range(len(column)), type=pa.int64())})
    router = Router(RoutingConfig(field="company", key_columns=[], seed_mode="scan"))
    actual_routed, actual_unrouted = router.split_and_count(table, values)
    expected_routed, expected_unrouted = _oracle_split_and_count(router, table, values)
    assert actual_unrouted == expected_unrouted
    assert set(actual_routed) == set(expected_routed)
    for val in expected_routed:
        _assert_tables_equal(actual_routed[val], expected_routed[val], f"typed-router[{val}]")


def test_split_and_count_rejects_converted_value_collision():
    """'1' and '01' both convert to int 1. The predecessor silently delivered
    the same rows to BOTH destinations (double delivery); the vectorized
    router rejects the configuration instead. Deliberate divergence."""
    from viaduck.router import RoutingError as RErr

    table = pa.table({"company": pa.array([1, 2], type=pa.int64())})
    router = Router(RoutingConfig(field="company", key_columns=[], seed_mode="scan"))
    with pytest.raises(RErr, match="collide"):
        router.split_and_count(table, ["1", "01"])


@pytest.mark.parametrize("phase", ["preimages", "conflicts"])
def test_null_rowid_rejected(phase):
    """Null rowids violate the stable-rowid contract; Arrow hash joins would
    silently misclassify them (the predecessor's dicts keyed None), so the
    vectorized phases fail fast instead. Deliberate divergence."""
    batch = _table(["update_preimage", "insert"], [None, 2], ["a", "b"])
    with pytest.raises(ValueError, match="null rowid"):
        if phase == "preimages":
            _resolve_preimages(batch, "company", [])
        else:
            _resolve_conflicts(batch)


def _counter_value(name: str) -> float:
    from prometheus_client import REGISTRY

    return REGISTRY.get_sample_value(name, {"pipeline": "test"}) or 0.0


def test_metric_parity_locked():
    """Counter-delta lock: per-ROW orphan/mutation increments, per-ROWID
    conflict increments — the predecessor's exact semantics."""
    # 2 orphan rows (one rowid twice), 1 mutated row.
    batch = _table(
        ["update_preimage", "update_preimage", "update_preimage", "update_postimage"],
        [1, 1, 2, 2],
        ["a", "a", "a", "b"],
    )
    orphans_before = _counter_value("viaduck_cdc_orphaned_preimages_total")
    mutations_before = _counter_value("viaduck_cdc_routing_mutations_total")
    _resolve_preimages(batch, "company", [])
    assert _counter_value("viaduck_cdc_orphaned_preimages_total") - orphans_before == 2
    assert _counter_value("viaduck_cdc_routing_mutations_total") - mutations_before == 1

    # Conflicts: rowid 1 = insert+delete (1 inc), rowid 2 = insert+postimage
    # no delete (1 inc), rowid 3 = postimage+delete only (0 inc).
    conflict_batch = _table(
        ["insert", "delete", "insert", "update_postimage", "update_postimage", "delete"],
        [1, 1, 2, 2, 3, 3],
        ["a"] * 6,
    )
    conflicts_before = _counter_value("viaduck_cdc_conflicts_resolved_total")
    _resolve_conflicts(conflict_batch)
    assert _counter_value("viaduck_cdc_conflicts_resolved_total") - conflicts_before == 2
