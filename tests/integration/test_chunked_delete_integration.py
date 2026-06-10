"""Integration locks for chunked deletes in _apply_changes.

_apply_changes splits deletes into _DELETE_CHUNK_ROWS-sized delete() calls
inside one destination transaction. These tests drive >1 chunk through a
real pyducklake catalog (local DuckDB, no Postgres/Docker): boundary sizes
around the chunk limit, composite keys spanning chunks, and NULL keys
distributed across chunks.
"""

from __future__ import annotations

import os

import pyarrow as pa
import pytest
from pyducklake import Catalog, Schema
from pyducklake.types import IntegerType, NestedField, StringType

from viaduck import metrics
from viaduck.main import _DELETE_CHUNK_ROWS, _apply_changes

pytestmark = pytest.mark.integration


def setup_module():
    metrics.init("integration_test")


def _make_catalog(tmp_path, name: str) -> Catalog:
    base = tmp_path / name
    meta_db = str(base / "meta.duckdb")
    data_dir = str(base / "data")
    os.makedirs(data_dir, exist_ok=True)
    return Catalog(name, meta_db, data_path=data_dir)


SCHEMA = Schema(
    NestedField(field_id=1, name="event_id", field_type=IntegerType(), required=True),
    NestedField(field_id=2, name="region", field_type=StringType()),
    NestedField(field_id=3, name="value", field_type=IntegerType()),
)


def _seed_rows(n: int, *, null_region_every: int | None = None) -> pa.Table:
    regions: list[str | None] = []
    for i in range(n):
        if null_region_every and i % null_region_every == 0:
            regions.append(None)
        else:
            regions.append(f"r{i % 7}")
    return pa.table(
        {
            "event_id": pa.array(range(n), type=pa.int32()),
            "region": pa.array(regions, type=pa.string()),
            "value": pa.array([i * 10 for i in range(n)], type=pa.int32()),
        }
    )


def _delete_batch(rows: pa.Table) -> pa.Table:
    return rows.append_column("change_type", pa.array(["delete"] * rows.num_rows, type=pa.string()))


@pytest.fixture()
def dest(tmp_path):
    catalog = _make_catalog(tmp_path, "dest")
    table = catalog.create_table("events", SCHEMA)
    yield catalog, table
    catalog.close()


@pytest.mark.parametrize(
    "n_delete",
    [
        _DELETE_CHUNK_ROWS - 1,  # single chunk, just under
        _DELETE_CHUNK_ROWS,  # exactly one chunk
        _DELETE_CHUNK_ROWS + 1,  # boundary: second chunk of 1 row
        2 * _DELETE_CHUNK_ROWS + 137,  # three chunks, ragged tail
    ],
)
def test_single_key_delete_across_chunk_boundaries(dest, n_delete):
    catalog, table = dest
    n_total = n_delete + 50
    seed = _seed_rows(n_total)
    table.append(seed)

    to_delete = seed.slice(0, n_delete)
    counts = _apply_changes(catalog, table, _delete_batch(to_delete), ["event_id"])
    assert counts["deleted"] == n_delete

    remaining = catalog.load_table("events").scan().to_arrow()
    assert remaining.num_rows == 50
    assert sorted(remaining.column("event_id").to_pylist()) == list(range(n_delete, n_total))


def test_composite_key_delete_spanning_chunks(dest):
    catalog, table = dest
    n_delete = _DELETE_CHUNK_ROWS + 25
    n_total = n_delete + 40
    seed = _seed_rows(n_total)
    table.append(seed)

    to_delete = seed.slice(0, n_delete)
    counts = _apply_changes(catalog, table, _delete_batch(to_delete), ["event_id", "region"])
    assert counts["deleted"] == n_delete

    remaining = catalog.load_table("events").scan().to_arrow()
    assert remaining.num_rows == 40
    assert sorted(remaining.column("event_id").to_pylist()) == list(range(n_delete, n_total))


def test_null_keys_distributed_across_chunks(dest):
    """NULL key values land in multiple chunks: each chunk's filter gets its
    own IsNull branch (single-key path) / IsNull conjunct (composite path);
    the union must equal the unchunked semantics."""
    catalog, table = dest
    n_delete = _DELETE_CHUNK_ROWS + 30
    n_total = n_delete + 60
    # Null region every 100 rows → nulls in both chunk 1 and chunk 2.
    seed = _seed_rows(n_total, null_region_every=100)
    table.append(seed)

    to_delete = seed.slice(0, n_delete).select(["region", "event_id", "value"])
    counts = _apply_changes(catalog, table, _delete_batch(to_delete), ["event_id", "region"])
    assert counts["deleted"] == n_delete

    remaining = catalog.load_table("events").scan().to_arrow()
    assert remaining.num_rows == 60
    assert sorted(remaining.column("event_id").to_pylist()) == list(range(n_delete, n_total))


def test_delete_plus_upsert_same_transaction_across_chunks(dest):
    """Chunked deletes + the upsert still commit as one transaction; the
    upserted rows must not be eaten by any delete chunk."""
    catalog, table = dest
    n_delete = _DELETE_CHUNK_ROWS + 10
    seed = _seed_rows(n_delete)
    table.append(seed)

    deletes = _delete_batch(seed)
    upserts = pa.table(
        {
            "event_id": pa.array([10_000, 10_001], type=pa.int32()),
            "region": pa.array(["new", "new"], type=pa.string()),
            "value": pa.array([1, 2], type=pa.int32()),
            "change_type": pa.array(["insert", "insert"], type=pa.string()),
        }
    )
    batch = pa.concat_tables([deletes, upserts], promote_options="default")
    counts = _apply_changes(catalog, table, batch, ["event_id"])
    assert counts["deleted"] == n_delete
    assert counts["upserted"] == 2

    remaining = catalog.load_table("events").scan().to_arrow()
    assert sorted(remaining.column("event_id").to_pylist()) == [10_000, 10_001]
