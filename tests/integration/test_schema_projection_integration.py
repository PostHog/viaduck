"""End-to-end integration test for the events_nrt → canonical events switchover.

Replicates exactly what the production switchover will look like:
  - Source table shaped like `posthog.events_nrt_beta` on a production tenant duckling:
    26 columns, all-varchar timestamps, has `captured_at`, unpartitioned.
    This is what millpond currently writes and what viaduck currently reads.
  - Destination table shaped like canonical `posthog.events`:
    25 columns, timestamptz on 8 columns, no `captured_at`, partitioned by
    (year(timestamp), month(timestamp), day(timestamp)). This is what DLT
    backfills onto duckling-*.
  - A realistic CDC batch from the read path (i.e., after
    millpond has written the source rows).
  - viaduck's `append_only` path with `schema_projection_enabled=True`.
  - Post-write assertions on the destination table: row count, per-column
    types, `captured_at` absence, timestamp values round-trip through the
    varchar → timestamptz cast at the correct UTC moment.

If this test breaks, the switchover is broken. It is deliberately the
loudest signal we can build without spinning up the real Kafka + millpond
+ duckling stack in Docker — those live checks stay in staging.

No Docker or Postgres — uses local DuckDB catalogs in tmp_path, matching
the existing `test_replication_integration.py` pattern.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from pyducklake import Catalog, Schema
from pyducklake.types import (
    BigIntType,
    BooleanType,
    NestedField,
    StringType,
    TimestampTZType,
)

from viaduck import metrics
from viaduck.apply import append_only
from viaduck.schema_projection import build

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Schemas — mirror the real tenant duckling catalog exactly.
# ---------------------------------------------------------------------------


def _events_nrt_source_schema() -> Schema:
    """26-column raw-Kafka shape millpond writes to events_nrt* today.

    All timestamp-shaped columns arrive as strings from Kafka JSON (JSON has
    no timestamp type). `_inserted_at` is the exception — millpond synthesizes
    it via `NOW() AS _inserted_at` in its INSERT, so it lands as timestamptz.
    """
    return Schema(
        NestedField(field_id=1, name="uuid", field_type=StringType()),
        NestedField(field_id=2, name="event", field_type=StringType()),
        NestedField(field_id=3, name="properties", field_type=StringType()),
        NestedField(field_id=4, name="timestamp", field_type=StringType()),
        NestedField(field_id=5, name="team_id", field_type=BigIntType()),
        NestedField(field_id=6, name="project_id", field_type=BigIntType()),
        NestedField(field_id=7, name="distinct_id", field_type=StringType()),
        NestedField(field_id=8, name="elements_chain", field_type=StringType()),
        NestedField(field_id=9, name="created_at", field_type=StringType()),
        NestedField(field_id=10, name="captured_at", field_type=StringType()),  # ← dropped by projection
        NestedField(field_id=11, name="person_id", field_type=StringType()),
        NestedField(field_id=12, name="person_properties", field_type=StringType()),
        NestedField(field_id=13, name="person_created_at", field_type=StringType()),
        NestedField(field_id=14, name="person_mode", field_type=StringType()),
        NestedField(field_id=15, name="historical_migration", field_type=BooleanType()),
        NestedField(field_id=16, name="group0_properties", field_type=StringType()),
        NestedField(field_id=17, name="group1_properties", field_type=StringType()),
        NestedField(field_id=18, name="group2_properties", field_type=StringType()),
        NestedField(field_id=19, name="group3_properties", field_type=StringType()),
        NestedField(field_id=20, name="group4_properties", field_type=StringType()),
        NestedField(field_id=21, name="group0_created_at", field_type=StringType()),
        NestedField(field_id=22, name="group1_created_at", field_type=StringType()),
        NestedField(field_id=23, name="group2_created_at", field_type=StringType()),
        NestedField(field_id=24, name="group3_created_at", field_type=StringType()),
        NestedField(field_id=25, name="group4_created_at", field_type=StringType()),
        NestedField(field_id=26, name="_inserted_at", field_type=TimestampTZType()),
    )


def _events_canonical_target_schema() -> Schema:
    """25-column DLT-backfilled canonical shape.

    Column order matches the real the tenant duckling's posthog.events (see
    viaduck-canonical-events-switchover.md for the diff table). Note the
    positions of person_mode (col 23) and the group_*_created_at cluster
    (cols 18-22) — moving these around under a positional INSERT would
    silently write into the wrong slots.
    """
    return Schema(
        NestedField(field_id=1, name="uuid", field_type=StringType()),
        NestedField(field_id=2, name="event", field_type=StringType()),
        NestedField(field_id=3, name="properties", field_type=StringType()),
        NestedField(field_id=4, name="timestamp", field_type=TimestampTZType()),
        NestedField(field_id=5, name="team_id", field_type=BigIntType()),
        NestedField(field_id=6, name="project_id", field_type=BigIntType()),
        NestedField(field_id=7, name="distinct_id", field_type=StringType()),
        NestedField(field_id=8, name="elements_chain", field_type=StringType()),
        NestedField(field_id=9, name="created_at", field_type=TimestampTZType()),
        NestedField(field_id=10, name="person_id", field_type=StringType()),
        NestedField(field_id=11, name="person_created_at", field_type=TimestampTZType()),
        NestedField(field_id=12, name="person_properties", field_type=StringType()),
        NestedField(field_id=13, name="group0_properties", field_type=StringType()),
        NestedField(field_id=14, name="group1_properties", field_type=StringType()),
        NestedField(field_id=15, name="group2_properties", field_type=StringType()),
        NestedField(field_id=16, name="group3_properties", field_type=StringType()),
        NestedField(field_id=17, name="group4_properties", field_type=StringType()),
        NestedField(field_id=18, name="group0_created_at", field_type=TimestampTZType()),
        NestedField(field_id=19, name="group1_created_at", field_type=TimestampTZType()),
        NestedField(field_id=20, name="group2_created_at", field_type=TimestampTZType()),
        NestedField(field_id=21, name="group3_created_at", field_type=TimestampTZType()),
        NestedField(field_id=22, name="group4_created_at", field_type=TimestampTZType()),
        NestedField(field_id=23, name="person_mode", field_type=StringType()),
        NestedField(field_id=24, name="historical_migration", field_type=BooleanType()),
        NestedField(field_id=25, name="_inserted_at", field_type=TimestampTZType()),
    )


# ---------------------------------------------------------------------------
# Fixture data — 3 realistic rows spanning 2 (year, month, day) partitions
# so we exercise the partition-value derivation on the write side.
# ---------------------------------------------------------------------------


TS_STR_A = "2026-06-29 12:00:00.000"
TS_STR_B = "2026-06-30 12:00:00.000"


def _realistic_source_batch() -> pa.Table:
    """3 rows of realistic raw-Kafka events matching the source schema.

    Uses ClickHouse's actual producer format for timestamps: ISO-8601 with
    space separator + 3 fractional digits (matches the `_TIMESTAMPTZ` cast
    path in millpond's arrow_converter._to_timestamptz).
    """
    return pa.table(
        {
            "uuid": ["u1", "u2", "u3"],
            "event": ["$pageview", "$identify", "$pageview"],
            "properties": ['{"$os":"macos"}', '{"$os":"linux"}', '{"$os":"windows"}'],
            "timestamp": [TS_STR_A, TS_STR_A, TS_STR_B],
            "team_id": [50689, 50689, 50689],
            "project_id": [50689, 50689, 50689],
            "distinct_id": ["d1", "d2", "d3"],
            "elements_chain": ["", "", "a > b"],
            "created_at": [TS_STR_A, TS_STR_A, TS_STR_B],
            "captured_at": [TS_STR_A, TS_STR_A, TS_STR_B],
            "person_id": ["p1", "p2", "p3"],
            "person_properties": ["{}", "{}", "{}"],
            "person_created_at": [TS_STR_A, TS_STR_A, TS_STR_B],
            "person_mode": ["propertyless", "full", "propertyless"],
            "historical_migration": [False, False, False],
            "group0_properties": ["{}", "{}", "{}"],
            "group1_properties": ["{}", "{}", "{}"],
            "group2_properties": ["{}", "{}", "{}"],
            "group3_properties": ["{}", "{}", "{}"],
            "group4_properties": ["{}", "{}", "{}"],
            "group0_created_at": [TS_STR_A, TS_STR_A, TS_STR_B],
            "group1_created_at": [TS_STR_A, TS_STR_A, TS_STR_B],
            "group2_created_at": [TS_STR_A, TS_STR_A, TS_STR_B],
            "group3_created_at": [TS_STR_A, TS_STR_A, TS_STR_B],
            "group4_created_at": [TS_STR_A, TS_STR_A, TS_STR_B],
            "_inserted_at": pa.array(
                [datetime(2026, 6, 30, 15, 0, tzinfo=UTC)] * 3,
                type=pa.timestamp("us", tz="UTC"),
            ),
        },
        schema=_events_nrt_source_schema().as_arrow(),
    )


# ---------------------------------------------------------------------------
# Test infrastructure
# ---------------------------------------------------------------------------


def _make_catalog(tmp_path, name: str) -> Catalog:
    base = tmp_path / name
    meta_db = str(base / "meta.duckdb")
    data_dir = str(base / "data")
    os.makedirs(data_dir, exist_ok=True)
    return Catalog(name, meta_db, data_path=data_dir)


def _read_all(table) -> pa.Table:
    return table.scan().to_arrow()


def setup_module():
    metrics.init("schema_projection_integration_test")


@pytest.fixture()
def source_table(tmp_path):
    cat = _make_catalog(tmp_path, "source_events_nrt")
    try:
        yield cat.create_table("events_nrt", _events_nrt_source_schema())
    finally:
        cat.close()


@pytest.fixture()
def dest_catalog(tmp_path):
    cat = _make_catalog(tmp_path, "dest_events")
    yield cat
    cat.close()


@pytest.fixture()
def dest_table(dest_catalog):
    """Destination shaped like canonical events. Not partitioned in this
    test — DuckLake's `create_table` doesn't take a partition spec, and
    partition testing lives in the pool-side path (destination._ensure_partition_spec).
    Type coercion + column dropping is the tested surface here."""
    return dest_catalog.create_table("events", _events_canonical_target_schema())


# ---------------------------------------------------------------------------
# End-to-end tests
# ---------------------------------------------------------------------------


def test_switchover_write_lands_correct_shape_and_values(source_table, dest_catalog, dest_table):
    """The load-bearing switchover test.

    Given a batch shaped like millpond's events_nrt output, a projection
    built from the source vs canonical events schemas, and viaduck's
    append_only path — verify that after one flush:

      - The destination has exactly N rows (row count preserved).
      - Every destination column has its target type (types coerced correctly).
      - `captured_at` is not present on the destination (dropped as intended).
      - Timestamp string values round-trip to the correct UTC moment
        (the varchar → timestamptz cast is doing what the millpond
        arrow_converter path does).
      - Column values land in the correct slots — testing this catches
        the silent-corruption bug where positional INSERT under a
        column-order mismatch would put `person_properties` values into
        the `person_created_at` slot.
    """
    # Build a real ProjectionPlan from the actual schemas.
    plan = build(
        source_schema=_events_nrt_source_schema().as_arrow(),
        target_schema=_events_canonical_target_schema().as_arrow(),
        drop_allow={"captured_at"},
    )

    # Set up a mock DestinationPool. append_only calls:
    #   dest_pool.projection_for(dest_id) → plan
    #   dest_pool.get(dest_id) → (catalog, table)
    #   dest_pool.release(dest_id) → noop
    # Using MagicMock is fine here because append_only's contract with the
    # pool is only these three methods.
    pool = MagicMock()
    pool.projection_for.return_value = plan
    pool.get.return_value = (dest_catalog, dest_table)

    batch = _realistic_source_batch()
    assert batch.num_rows == 3

    written = append_only(pool, "dest-1", batch)
    assert written == 3

    # Read the destination back through pyducklake.
    dest_rows = _read_all(dest_table)

    # Row count preserved.
    assert dest_rows.num_rows == 3

    # Schema matches target — call out the fields explicitly for a
    # readable failure message rather than relying on pa.Schema.equals()
    # (which produces a wall of text).
    target = _events_canonical_target_schema().as_arrow()
    assert dest_rows.column_names == target.names, (
        f"dest column names do not match target:\n  got: {dest_rows.column_names}\n  want: {target.names}"
    )
    for field in target:
        got_type = dest_rows.schema.field(field.name).type
        # DuckDB reports timestamptz columns in the caller's local tz on read
        # (values are UTC epochs internally, the tz label is display-only). For
        # timestamptz columns, only assert that the kind is timestamp-with-tz;
        # for non-timestamptz columns, require exact-type match.
        if pa.types.is_timestamp(field.type) and field.type.tz is not None:
            assert pa.types.is_timestamp(got_type) and got_type.tz is not None, (
                f"column {field.name!r}: expected tz-aware timestamp, got {got_type}"
            )
        else:
            assert got_type == field.type, f"column {field.name!r}: got {got_type}, want {field.type}"

    # captured_at is not present on the destination.
    assert "captured_at" not in dest_rows.column_names

    # Timestamp values round-trip correctly to the exact UTC moment.
    expected_ts_a = datetime(2026, 6, 29, 12, 0, 0, tzinfo=UTC)
    expected_ts_b = datetime(2026, 6, 30, 12, 0, 0, tzinfo=UTC)
    ts_col = dest_rows.column("timestamp").to_pylist()
    assert ts_col == [expected_ts_a, expected_ts_a, expected_ts_b], f"timestamp values wrong: got {ts_col}"

    # Value-slot correctness: `person_mode` has known non-null strings that
    # would be catastrophic if written into a different column. Confirm each
    # row's person_mode value ended up in the person_mode slot.
    person_mode_col = dest_rows.column("person_mode").to_pylist()
    assert person_mode_col == ["propertyless", "full", "propertyless"], (
        f"person_mode landed in the wrong slot: got {person_mode_col}"
    )

    # Value-slot correctness on a timestamptz-cast column: person_created_at
    # should carry the source timestamps parsed to UTC, not string values or
    # person_id values (the columns positioned nearby that could be
    # mis-written under a positional-insert bug).
    person_created_at = dest_rows.column("person_created_at").to_pylist()
    assert person_created_at == [expected_ts_a, expected_ts_a, expected_ts_b], (
        f"person_created_at landed wrong: got {person_created_at}"
    )


def test_switchover_identity_projection_zero_overhead(source_table):
    """Sanity: when source_schema == target_schema, the pool returns None
    (see DestinationPool.projection_for) and append_only skips the
    projection call entirely. Confirms we don't pay the projection cost on
    the identity path."""
    src_arrow = _events_nrt_source_schema().as_arrow()
    plan = build(src_arrow, src_arrow)

    assert plan.is_identity
    # projection_for() collapses is_identity plans to None so the write path
    # never calls .apply() on the hot identity case.
    import threading

    from viaduck.destination import DestinationPool  # noqa: PLC0415

    pool = DestinationPool.__new__(DestinationPool)
    pool._projections = {"d1": plan}
    pool._lock = threading.Lock()
    assert pool.projection_for("d1") is None


def test_full_cdc_delete_path_matches_upserted_rows_through_projection(tmp_path):
    """End-to-end: full_cdc with a key column that survives the projection
    (same type on both sides) must correctly delete-match rows that were
    previously upserted through the same projection. The projected upsert
    and the projected delete must produce the SAME key value for the delete
    filter to hit. Covers the QE-flagged "delete-path + projection" seam.
    """
    from unittest.mock import MagicMock  # noqa: PLC0415

    from pyducklake.schema import NestedField, Schema  # noqa: PLC0415
    from pyducklake.types import StringType, TimestampTZType  # noqa: PLC0415

    from viaduck.apply import apply_full_cdc  # noqa: PLC0415

    # Source: id (key, string), val (string, will be dropped), _inserted_at.
    # Target: id, _inserted_at.  Key column is bijective (string→string).
    src_schema = Schema(
        NestedField(1, "id", StringType(), required=True),
        NestedField(2, "val", StringType(), required=False),
        NestedField(3, "_inserted_at", TimestampTZType(), required=True),
    )
    tgt_schema = Schema(
        NestedField(1, "id", StringType(), required=True),
        NestedField(2, "_inserted_at", TimestampTZType(), required=True),
    )

    src_arrow = src_schema.as_arrow()
    tgt_arrow = tgt_schema.as_arrow()
    plan = build(src_arrow, tgt_arrow, drop_allow={"val"}, key_columns=("id",))

    # Build a real dest table.
    cat = _make_catalog(tmp_path, "dest_cdc")
    try:
        dest_table = cat.create_table("events", tgt_schema)

        pool = MagicMock()
        pool.projection_for.return_value = plan
        pool.get.return_value = (cat, dest_table)

        # Batch 1: two inserts (id=a, id=b). CDC-shape includes change_type,
        # rowid, and _snapshot_id metadata columns that strip_meta will remove
        # before projection runs.
        ts_utc = datetime(2026, 6, 30, 12, 0, 0, tzinfo=UTC)
        insert_batch = pa.table(
            {
                "change_type": ["insert", "insert"],
                "rowid": pa.array([1, 2], type=pa.uint64()),
                "snapshot_id": pa.array([1, 1], type=pa.uint64()),
                "id": ["a", "b"],
                "val": ["v-a", "v-b"],
                "_inserted_at": [ts_utc, ts_utc],
            }
        )
        apply_full_cdc(pool, "dest-1", insert_batch, ["id"])

        rows = _read_all(dest_table)
        assert set(rows.column("id").to_pylist()) == {"a", "b"}

        # Batch 2: delete of id=a. The projected delete-row must carry id=a
        # so the delete filter matches the row we just wrote.
        delete_batch = pa.table(
            {
                "change_type": ["delete"],
                "rowid": pa.array([1], type=pa.uint64()),
                "snapshot_id": pa.array([2], type=pa.uint64()),
                "id": ["a"],
                "val": ["v-a"],
                "_inserted_at": [ts_utc],
            }
        )
        apply_full_cdc(pool, "dest-1", delete_batch, ["id"])

        remaining = _read_all(dest_table)
        assert remaining.column("id").to_pylist() == ["b"], (
            f"delete-path failed to match projected key: remaining {remaining.column('id').to_pylist()}"
        )
    finally:
        cat.close()


def test_switchover_raises_at_build_on_unknown_source_column(source_table, dest_table):
    """Sanity: if a source column that neither exists in target nor appears in
    dropSourceColumns arrives, projection build raises immediately (not at
    first flush after 30 minutes of accumulated batches)."""
    src = _events_nrt_source_schema().as_arrow()
    tgt = _events_canonical_target_schema().as_arrow()
    from viaduck.schema_projection import SchemaProjectionError  # noqa: PLC0415

    with pytest.raises(SchemaProjectionError, match="captured_at"):
        # No drop_allow → captured_at is an unresolvable orphan.
        build(source_schema=src, target_schema=tgt)
