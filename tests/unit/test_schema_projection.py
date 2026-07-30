"""Tests for viaduck.schema_projection.

The load-bearing fixture is the real events_nrt → canonical events diff
we captured on a production tenant duckling: 26 → 25 columns, 8 varchar→timestamptz
casts, extra `captured_at` in source, and enough column-order shuffling
that positional insert without projection would silently write into the
wrong slots.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pyarrow as pa
import pytest

from viaduck.schema_projection import SchemaProjectionError, build

# ---- Real-shape fixtures ----------------------------------------------------
#
# These match the schemas queried directly from the DuckLake catalogs on
# a production tenant duckling: posthog.events_nrt_beta (raw millpond Kafka shape) and
# posthog.events (DLT-backfilled canonical shape). See
# viaduck-canonical-events-switchover.md for the diff table.


def _source_events_nrt_schema() -> pa.Schema:
    """The 26-column raw-Kafka shape millpond writes to events_nrt* today."""
    return pa.schema(
        [
            pa.field("uuid", pa.string()),
            pa.field("event", pa.string()),
            pa.field("properties", pa.string()),
            pa.field("timestamp", pa.string()),
            pa.field("team_id", pa.int64()),
            pa.field("project_id", pa.int64()),
            pa.field("distinct_id", pa.string()),
            pa.field("elements_chain", pa.string()),
            pa.field("created_at", pa.string()),
            pa.field("captured_at", pa.string()),  # ← absent from canonical events
            pa.field("person_id", pa.string()),
            pa.field("person_properties", pa.string()),
            pa.field("person_created_at", pa.string()),
            pa.field("person_mode", pa.string()),
            pa.field("historical_migration", pa.bool_()),
            pa.field("group0_properties", pa.string()),
            pa.field("group1_properties", pa.string()),
            pa.field("group2_properties", pa.string()),
            pa.field("group3_properties", pa.string()),
            pa.field("group4_properties", pa.string()),
            pa.field("group0_created_at", pa.string()),
            pa.field("group1_created_at", pa.string()),
            pa.field("group2_created_at", pa.string()),
            pa.field("group3_created_at", pa.string()),
            pa.field("group4_created_at", pa.string()),
            pa.field("_inserted_at", pa.timestamp("us", tz="UTC")),
        ]
    )


def _target_events_schema() -> pa.Schema:
    """The 25-column canonical shape DLT backfilled onto the tenant duckling's posthog.events."""
    utc = pa.timestamp("us", tz="UTC")
    return pa.schema(
        [
            pa.field("uuid", pa.string()),
            pa.field("event", pa.string()),
            pa.field("properties", pa.string()),
            pa.field("timestamp", utc),
            pa.field("team_id", pa.int64()),
            pa.field("project_id", pa.int64()),
            pa.field("distinct_id", pa.string()),
            pa.field("elements_chain", pa.string()),
            pa.field("created_at", utc),
            pa.field("person_id", pa.string()),
            pa.field("person_created_at", utc),
            pa.field("person_properties", pa.string()),
            pa.field("group0_properties", pa.string()),
            pa.field("group1_properties", pa.string()),
            pa.field("group2_properties", pa.string()),
            pa.field("group3_properties", pa.string()),
            pa.field("group4_properties", pa.string()),
            pa.field("group0_created_at", utc),
            pa.field("group1_created_at", utc),
            pa.field("group2_created_at", utc),
            pa.field("group3_created_at", utc),
            pa.field("group4_created_at", utc),
            pa.field("person_mode", pa.string()),
            pa.field("historical_migration", pa.bool_()),
            pa.field("_inserted_at", utc),
        ]
    )


def _sample_batch_matching_source(source_schema: pa.Schema, n_rows: int = 3) -> pa.Table:
    """Build a batch with valid values for every source column."""
    ts_str = (
        "2026-06-30 12:34:56.789"  # ISO-8601 with space separator + 3 fractional digits — ClickHouse producer format
    )
    data = {}
    for f in source_schema:
        if pa.types.is_string(f.type):
            data[f.name] = [ts_str if "at" in f.name or f.name == "timestamp" else f"val_{i}" for i in range(n_rows)]
        elif pa.types.is_integer(f.type):
            data[f.name] = list(range(n_rows))
        elif pa.types.is_boolean(f.type):
            data[f.name] = [False] * n_rows
        elif pa.types.is_timestamp(f.type):
            data[f.name] = [datetime(2026, 6, 30, tzinfo=UTC)] * n_rows
        else:
            raise ValueError(f"Sample generator doesn't know how to fill {f.type}")
    return pa.table(data, schema=source_schema)


# ---- Load-bearing tests -----------------------------------------------------


def test_events_nrt_to_events_projects_to_target_schema_exactly():
    """The real events_nrt → events diff must produce a batch whose schema equals
    the target schema exactly, and preserve row count. If this test breaks, the
    switchover to canonical events is silently broken.
    """
    source = _source_events_nrt_schema()
    target = _target_events_schema()

    plan = build(source, target, drop_allow={"captured_at"})
    batch = _sample_batch_matching_source(source, n_rows=5)

    result = plan.apply(batch)

    assert result.schema.equals(target), (
        f"projected schema does not match target:\n  got: {result.schema}\n  want: {target}"
    )
    assert len(result) == 5


def test_plan_summarizes_the_real_diff():
    """describe() should call out the load-bearing changes for auditability."""
    plan = build(_source_events_nrt_schema(), _target_events_schema(), drop_allow={"captured_at"})
    summary = plan.describe()

    assert "captured_at" in summary
    # 8 timestamps get cast; call at least one out
    assert "timestamp:" in summary
    assert "created_at:" in summary


def test_timestamps_land_in_correct_utc_slot():
    """Cast varchar → timestamptz must preserve the encoded moment, stamped UTC.
    Uses the ClickHouse producer's space-separator + 3-fractional-digit format.
    """
    source = _source_events_nrt_schema()
    target = _target_events_schema()
    plan = build(source, target, drop_allow={"captured_at"})

    batch = _sample_batch_matching_source(source, n_rows=1)
    # Overwrite timestamp with a known, distinctive value.
    batch = batch.set_column(
        batch.schema.get_field_index("timestamp"),
        "timestamp",
        pa.array(["2026-06-30 12:34:56.789"], type=pa.string()),
    )

    result = plan.apply(batch)
    ts = result.column("timestamp")[0].as_py()

    assert ts == datetime(2026, 6, 30, 12, 34, 56, 789000, tzinfo=UTC)


def test_column_order_matches_target():
    """Positional INSERT means order matters. Confirm the projected batch's
    column order is exactly the target's."""
    source = _source_events_nrt_schema()
    target = _target_events_schema()
    plan = build(source, target, drop_allow={"captured_at"})

    batch = _sample_batch_matching_source(source)
    result = plan.apply(batch)

    assert result.column_names == target.names


# ---- Build-time validation --------------------------------------------------


def test_unknown_source_column_not_in_allow_list_raises_at_build():
    """The whole point of this module: silent drops are how we got here."""
    source = pa.schema([("known", pa.string()), ("stowaway", pa.string())])
    target = pa.schema([("known", pa.string())])

    with pytest.raises(SchemaProjectionError, match="stowaway"):
        build(source, target)


def test_cast_on_not_null_target_raises_at_build():
    """T1: A cast column whose target is NOT NULL is a liveness hazard.
    The per-value fallback nulls unparseable values, and a NULL on a NOT NULL
    target rejects at DuckLake write time — the flush retries the same batch
    and stalls indefinitely. Refuse at build so operator sees the problem
    before deploy, not as replication lag in prod.
    """
    source = pa.schema([pa.field("t", pa.string())])
    target = pa.schema([pa.field("t", pa.timestamp("us", tz="UTC"), nullable=False)])

    with pytest.raises(SchemaProjectionError, match="NOT NULL and require a type cast"):
        build(source, target)


def test_cast_on_nullable_target_is_allowed():
    """Sanity: the T1 guard only fires for NOT NULL targets."""
    source = pa.schema([pa.field("t", pa.string())])
    target = pa.schema([pa.field("t", pa.timestamp("us", tz="UTC"), nullable=True)])

    plan = build(source, target)
    assert plan.cast_columns == (("t", pa.string(), pa.timestamp("us", tz="UTC")),)


def test_key_column_with_cast_raises_at_build():
    """A cast on a key column is unsafe: Phase 3 delete-matching depends on
    the exact insert-time value round-tripping to the delete-time filter. TLA
    review flagged this as violating NoPhantomWhenCurrent."""
    source = pa.schema([("id", pa.string()), ("val", pa.string())])
    target = pa.schema([("id", pa.int64()), ("val", pa.string())])

    with pytest.raises(SchemaProjectionError, match="Key columns.*id"):
        build(source, target, key_columns=("id",))


def test_non_key_drop_with_key_columns_is_allowed():
    """Drop of a non-key column doesn't trigger the key guard."""
    source = pa.schema([("id", pa.string()), ("captured_at", pa.string())])
    target = pa.schema([("id", pa.string())])

    plan = build(source, target, drop_allow={"captured_at"}, key_columns=("id",))

    assert plan.drop_columns == ("captured_at",)


def test_key_column_in_drop_allow_raises_at_build():
    """A key column dropped from destination cannot be used to delete-match."""
    source = pa.schema([("id", pa.string()), ("val", pa.string())])
    target = pa.schema([("val", pa.string())])

    with pytest.raises(SchemaProjectionError, match="Key columns.*id.*drop_source_columns"):
        build(source, target, drop_allow={"id"}, key_columns=("id",))


def test_key_column_null_fill_raises_at_build():
    """Key column absent from source would need null-fill; refuse — a null
    key can't identify rows for delete matching."""
    source = pa.schema([("val", pa.string())])
    target = pa.schema([pa.field("id", pa.string(), nullable=True), pa.field("val", pa.string())])

    with pytest.raises(SchemaProjectionError, match="Key columns.*id.*null-filled"):
        build(source, target, key_columns=("id",))


def test_routing_column_drop_raises_at_build():
    """Routing column dropped from destination violates PartitionCorrectness:
    routing decides which destination a row lands in, and the destination row
    must carry the routing value that matched RoutingMap[d]."""
    source = pa.schema([("team_id", pa.int64()), ("val", pa.string())])
    target = pa.schema([("val", pa.string())])

    with pytest.raises(SchemaProjectionError, match="Routing column 'team_id'.*drop_source_columns"):
        build(source, target, drop_allow={"team_id"}, routing_field="team_id")


def test_routing_column_cast_raises_at_build():
    """Cast on routing column may change its value and violate PartitionCorrectness."""
    source = pa.schema([("team_id", pa.string()), ("val", pa.string())])
    target = pa.schema([("team_id", pa.int64()), ("val", pa.string())])

    with pytest.raises(SchemaProjectionError, match="Routing column 'team_id'.*cast"):
        build(source, target, routing_field="team_id")


def test_routing_column_null_fill_raises_at_build():
    """Missing routing column would be null-filled → can't match RoutingMap[d]."""
    source = pa.schema([("val", pa.string())])
    target = pa.schema([pa.field("team_id", pa.int64(), nullable=True), pa.field("val", pa.string())])

    with pytest.raises(SchemaProjectionError, match="Routing column 'team_id'.*missing from source"):
        build(source, target, routing_field="team_id")


def test_target_not_null_with_no_source_raises_at_build():
    """A NOT NULL target column with no corresponding source is unrecoverable —
    null-fill would violate the constraint at write time. Raise now, not later."""
    source = pa.schema([("a", pa.string())])
    target = pa.schema(
        [
            pa.field("a", pa.string()),
            pa.field("b", pa.string(), nullable=False),
        ]
    )

    with pytest.raises(SchemaProjectionError, match="b"):
        build(source, target)


def test_drop_allow_column_that_is_present_in_target_is_ignored():
    """If a column is in target, it stays — even if the operator listed it in
    dropSourceColumns. Explicit drop can't override a target field."""
    source = pa.schema([("a", pa.string()), ("b", pa.string())])
    target = pa.schema([("a", pa.string()), ("b", pa.string())])

    plan = build(source, target, drop_allow={"a", "b"})

    assert plan.drop_columns == ()
    assert set(plan.passthrough_columns) == {"a", "b"}


def test_missing_source_column_null_fills_when_nullable():
    """The complement of null_fill: target has it, source doesn't, target is
    nullable → auto-null. This is the common case for a new destination
    column being introduced."""
    source = pa.schema([("a", pa.string())])
    target = pa.schema([("a", pa.string()), pa.field("b", pa.int64(), nullable=True)])

    plan = build(source, target)
    batch = pa.table({"a": ["x", "y"]}, schema=source)
    result = plan.apply(batch)

    assert result.column_names == ["a", "b"]
    assert result.column("b").to_pylist() == [None, None]


def test_identity_projection_is_flagged():
    """When source == target exactly, the plan should self-report as identity
    so callers can skip apply() on the hot path."""
    schema = pa.schema([("a", pa.string()), ("b", pa.int64())])
    plan = build(schema, schema)

    assert plan.is_identity
    assert plan.describe() == "identity"


def test_reordered_identical_fields_is_not_identity_and_reorders():
    """Same field-set + types but different column ORDER is NOT identity.
    pyducklake's positional INSERT would otherwise scramble slots — this is
    the load-bearing bug class the module exists to prevent."""
    src = pa.schema([("a", pa.string()), ("b", pa.int64())])
    tgt = pa.schema([("b", pa.int64()), ("a", pa.string())])

    plan = build(src, tgt)

    assert not plan.is_identity, "identity fast-path would skip the reorder"
    result = plan.apply(pa.table({"a": ["x"], "b": [1]}, schema=src))
    assert result.column_names == ["b", "a"]
    assert result.column("a").to_pylist() == ["x"]
    assert result.column("b").to_pylist() == [1]


def test_int_type_widening_uses_general_cast():
    """int32 → int64 is a common case that should go through the plain cast
    path (not the timestamp special-case). Ensures we don't accidentally
    steer numeric casts through the timestamp code."""
    source = pa.schema([("n", pa.int32())])
    target = pa.schema([("n", pa.int64())])
    plan = build(source, target)

    batch = pa.table({"n": pa.array([1, 2, 3], type=pa.int32())})
    result = plan.apply(batch)

    assert result.column("n").type == pa.int64()
    assert result.column("n").to_pylist() == [1, 2, 3]


def test_naive_timestamp_target_no_tz_stamp():
    """A varchar → naive-timestamp target should NOT get assume_timezone —
    only the tz-aware path uses it. Prevents accidental UTC-stamping when
    the target explicitly wants naive timestamps."""
    source = pa.schema([("t", pa.string())])
    target = pa.schema([("t", pa.timestamp("us"))])
    plan = build(source, target)

    batch = pa.table({"t": ["2026-06-30 12:00:00"]})
    result = plan.apply(batch)

    ts = result.column("t")[0].as_py()
    # Naive: no tzinfo attached
    assert ts.tzinfo is None
    assert ts == datetime(2026, 6, 30, 12, 0, 0)


def test_non_utc_tz_target_stamps_with_that_tz():
    """Cover the branch where target tz is set but not UTC — the assume_timezone
    call must use the target's actual tz, not a hardcoded UTC."""
    source = pa.schema([("t", pa.string())])
    target = pa.schema([("t", pa.timestamp("us", tz="America/Los_Angeles"))])
    plan = build(source, target)

    batch = pa.table({"t": ["2026-06-30 12:00:00"]})
    result = plan.apply(batch)

    ts = result.column("t")[0].as_py()
    # Result should carry an LA tz, not UTC. pyarrow returns a pytz zone here.
    assert ts.tzinfo is not None
    assert "Los_Angeles" in str(ts.tzinfo)


@pytest.mark.parametrize(
    "value,expects_null",
    [
        ("2026-06-30 12:34:56", False),  # ClickHouse canonical
        ("2026-06-30 12:34:56.789", False),  # ClickHouse canonical + ms
        ("", True),  # Empty string — producer-format drift
        ("NULL", True),  # Literal string "NULL"
        ("2026-06-30T12:34:56Z", True),  # Wrong ISO variant — no zone parser
        ("2026-06-30 12:00:00+00:00", True),  # Explicit offset — rejected by naive cast
        ("not-a-date", True),  # Junk
    ],
)
def test_timestamp_cast_per_value_fallback(value, expects_null):
    """Whole-batch cast fail-loud would stall the pipeline; per-value fallback
    nulls only the bad values and lets the batch through. Adversarial review
    flagged the whole-batch raise as a producer-format-drift outage vector.
    """
    source = pa.schema([("t", pa.string())])
    target = pa.schema([pa.field("t", pa.timestamp("us", tz="UTC"), nullable=True)])
    plan = build(source, target)

    batch = pa.table({"t": [value, "2026-06-30 12:34:56"]}, schema=source)

    fallback_events: list[tuple[str, int]] = []

    def _cb(column: str, count: int) -> None:
        fallback_events.append((column, count))

    result = plan.apply(batch, on_null_fallback=_cb)

    assert result.column("t")[1].as_py() == datetime(2026, 6, 30, 12, 34, 56, tzinfo=UTC), (
        "the good row must still parse correctly on the fallback path"
    )
    if expects_null:
        assert result.column("t")[0].as_py() is None, f"expected {value!r} to null"
        assert fallback_events == [("t", 1)], "callback should fire once with count=1"
    else:
        assert result.column("t")[0].as_py() is not None
        assert fallback_events == [], "clean batch should not fire callback"


def test_timestamp_cast_null_input_passes_through_fallback():
    """A None in the input should stay None on the fallback path (not count
    as a failure). Otherwise every batch with any legitimate NULLs would
    over-report drift and drown the alarm."""
    source = pa.schema([("t", pa.string())])
    target = pa.schema([pa.field("t", pa.timestamp("us", tz="UTC"), nullable=True)])
    plan = build(source, target)

    batch = pa.table({"t": [None, "not-a-date", "2026-06-30 12:00:00"]}, schema=source)

    events: list[tuple[str, int]] = []
    result = plan.apply(batch, on_null_fallback=lambda c, n: events.append((c, n)))

    assert result.column("t")[0].as_py() is None
    assert result.column("t")[1].as_py() is None
    assert result.column("t")[2].as_py() == datetime(2026, 6, 30, 12, 0, 0, tzinfo=UTC)
    assert events == [("t", 1)], "only the junk row counts, not the legit NULL"


def test_not_null_target_with_all_null_source_column_is_passthrough_at_build():
    """When the source has the column (so build() doesn't null-fill), an
    all-null batch will fail at the destination write (NOT NULL constraint).
    build() cannot detect this — nullability is batch-level, not schema-level.
    Documenting the boundary: build succeeds, downstream write is responsible
    for enforcing the constraint. QE flagged this to make the seam explicit.
    """
    source = pa.schema([pa.field("id", pa.string(), nullable=True)])
    target = pa.schema([pa.field("id", pa.string(), nullable=False)])

    plan = build(source, target)
    # No null_fill and no cast — just passthrough. Build cannot know values
    # are all-null; write path enforces NOT NULL.
    assert plan.null_fill_columns == ()
    assert plan.cast_columns == ()
    assert plan.passthrough_columns == ("id",)


def test_pyducklake_as_arrow_preserves_nullability_round_trip():
    """The NOT-NULL guard in build() reads `field.nullable` from the arrow
    schema. If pyducklake's `Schema.as_arrow()` (the shim we call in
    destination.py:250-251) silently sets everything to nullable, the NOT-NULL
    build guard becomes unreliable — a target's NOT NULL column that has no
    source would silently be null-filled at write time.
    """
    from pyducklake.schema import NestedField, Schema
    from pyducklake.types import StringType

    ducklake_schema = Schema(
        NestedField(1, "required_col", StringType(), required=True),
        NestedField(2, "nullable_col", StringType(), required=False),
    )

    arrow = ducklake_schema.as_arrow()

    assert arrow.field("required_col").nullable is False, (
        "pyducklake.Schema.as_arrow() must preserve NOT-NULL — build() relies on it"
    )
    assert arrow.field("nullable_col").nullable is True


def test_row_count_preserved_on_realistic_batch():
    """Sanity: N rows in → N rows out, no shuffling of null-fills across rows."""
    source = _source_events_nrt_schema()
    target = _target_events_schema()
    plan = build(source, target, drop_allow={"captured_at"})

    for n in [0, 1, 100, 1000]:
        batch = _sample_batch_matching_source(source, n_rows=n)
        result = plan.apply(batch)
        assert len(result) == n
