"""A source column of a type pyducklake can't represent must not break viaduck.

Live scenario: millpond's VARIANT dual-write adds `properties_variant VARIANT`
to `events_nrt` mid-stream. DuckDB (through at least 1.5.5) cannot export
VARIANT to Arrow, and pyducklake's type map cannot parse it — so before the
tolerant load + explicit read projection, that column stalled every CDC read
on running pods and crash-looped restarts (load_table ValueError).

No Postgres, no Docker. Uses tmp_path for the catalog.
"""

from __future__ import annotations

import os

import duckdb
import pyarrow as pa
import pytest
from pyducklake import Catalog, Schema
from pyducklake.types import IntegerType, NestedField, StringType

from viaduck import metrics
from viaduck.source import (
    current_snapshot_id,
    load_table,
    read_cdc,
    read_cdc_changes,
    replicated_column_names,
    strip_meta,
)

pytestmark = pytest.mark.integration

SOURCE_SCHEMA = Schema(
    NestedField(field_id=1, name="event_id", field_type=IntegerType(), required=True),
    NestedField(field_id=2, name="properties", field_type=StringType()),
)


def setup_module():
    metrics.init("integration_test")


@pytest.fixture()
def source_catalog(tmp_path):
    base = tmp_path / "source"
    os.makedirs(str(base / "data"), exist_ok=True)
    cat = Catalog("source", str(base / "meta.duckdb"), data_path=str(base / "data"))
    yield cat
    cat.close()


@pytest.fixture()
def variant_table(source_catalog):
    """events table with a VARIANT companion column and one committed row."""
    table = source_catalog.create_table("events", SOURCE_SCHEMA)
    table.append(
        pa.table(
            {
                "event_id": pa.array([1], type=pa.int32()),
                "properties": pa.array(['{"a": 1}'], type=pa.string()),
            }
        )
    )
    conn = source_catalog.connection
    conn.execute("ALTER TABLE source.main.events ADD COLUMN properties_variant VARIANT")
    conn.execute(
        "INSERT INTO source.main.events BY NAME "
        "(SELECT 2 AS event_id, '{\"b\": 2}' AS properties, "
        "try_cast(try_cast('{\"b\": 2}' AS JSON) AS VARIANT) AS properties_variant)"
    )
    return table


def test_load_table_excludes_unrepresentable_column(source_catalog, variant_table):
    # pyducklake's own loader raises — this is the CrashLoopBackOff this
    # feature exists to prevent. If this stops raising (pyducklake learns
    # VARIANT), the tolerant path below still works but could be revisited.
    with pytest.raises(ValueError, match="Cannot parse DuckDB type"):
        source_catalog.load_table("events")

    table = load_table(source_catalog, "events")
    names = replicated_column_names(table)
    assert names == ("event_id", "properties")
    assert "properties_variant" not in names


def test_cdc_reads_survive_variant_column(source_catalog, variant_table):
    table = load_table(source_catalog, "events")
    columns = replicated_column_names(table)
    head = current_snapshot_id(table)

    # Unprojected read is the fleet-wide silent stall: VARIANT cannot cross
    # the DuckDB→Arrow boundary. Canary assertion — if duckdb gains VARIANT
    # Arrow export, this test tells us the workaround is optional. The error
    # surfaces as duckdb.NotImplementedException or pyarrow-wrapped OSError
    # depending on which side of the boundary trips first.
    with pytest.raises((duckdb.NotImplementedException, OSError), match="Unsupported Arrow type VARIANT"):
        read_cdc(table, after_snapshot=0, end_snapshot=head)

    result = read_cdc(table, after_snapshot=0, end_snapshot=head, columns=columns)
    assert result.column_names == list(columns)
    assert sorted(result.column("event_id").to_pylist()) == [1, 2]

    changes = read_cdc_changes(table, after_snapshot=0, end_snapshot=head, columns=columns)
    assert set(columns) < set(changes.column_names)  # meta columns prepended
    assert "properties_variant" not in changes.column_names
    stripped = strip_meta(changes)
    assert stripped.column_names == list(columns)


def test_seed_scan_projection_survives_variant_column(source_catalog, variant_table):
    table = load_table(source_catalog, "events")
    columns = replicated_column_names(table)
    scanned = table.scan(selected_fields=columns).to_arrow()
    assert scanned.column_names == list(columns)
    assert scanned.num_rows == 2


def test_load_table_without_exotic_columns_is_unchanged(source_catalog):
    source_catalog.create_table("plain", SOURCE_SCHEMA)
    table = load_table(source_catalog, "plain")
    assert replicated_column_names(table) == ("event_id", "properties")
    # Same shape pyducklake's own loader produces (the happy path delegates).
    upstream = source_catalog.load_table("plain")
    assert [f.name for f in upstream.schema.fields] == list(replicated_column_names(table))


def test_required_column_with_excludable_type_fails_loudly(source_catalog, variant_table):
    # A routing/key column that would be excluded must fail startup, not
    # start "healthy" and break every flush downstream.
    with pytest.raises(ValueError, match="routing/key column"):
        load_table(source_catalog, "events", required_columns=("properties_variant",))


def test_non_allowlisted_unparseable_type_keeps_failing_loudly(source_catalog):
    # UHUGEINT is storable by ducklake but unparseable by pyducklake and NOT
    # in EXCLUDABLE_SOURCE_TYPES: exclusion tolerance is a narrow allowlist,
    # not a blanket except — anything unexpected keeps the old loud failure.
    source_catalog.create_table("with_uhugeint", SOURCE_SCHEMA)
    source_catalog.connection.execute("ALTER TABLE source.main.with_uhugeint ADD COLUMN u UHUGEINT")
    with pytest.raises(ValueError, match="Cannot parse DuckDB type"):
        load_table(source_catalog, "with_uhugeint")


def test_all_columns_excluded_fails_loudly(source_catalog):
    # An empty surviving schema would render 'SELECT  FROM ...' per poll.
    conn = source_catalog.connection
    conn.execute("CREATE TABLE source.main.only_variant (v VARIANT)")
    with pytest.raises(ValueError, match="refusing an empty schema"):
        load_table(source_catalog, "only_variant")


def test_quote_bearing_column_name_falls_back_to_unprojected(source_catalog):
    # Projection can't escape embedded quotes (pyducklake interpolates
    # f'"{name}"' verbatim), so replicated_column_names opts out and reads
    # revert to the pre-existing SELECT * behavior.
    conn = source_catalog.connection
    conn.execute('CREATE TABLE source.main.quoted ("a""b" INTEGER, x VARCHAR)')
    table = load_table(source_catalog, "quoted")
    assert replicated_column_names(table) is None


def test_quote_bearing_name_with_exclusions_fails_loudly(source_catalog):
    # When exclusions make projection load-bearing, an unprojectable name
    # must be a startup error, not malformed SQL every poll.
    conn = source_catalog.connection
    conn.execute('CREATE TABLE source.main.quoted_variant ("a""b" INTEGER, v VARIANT)')
    with pytest.raises(ValueError, match="embeds a double quote"):
        load_table(source_catalog, "quoted_variant")


def test_metric_failure_does_not_break_exclusion(source_catalog, variant_table, monkeypatch):
    # load_table's purpose is surviving the column — an unbound/broken
    # metric (e.g. metrics.init() not yet called) must not resurrect the crash.
    class _Boom:
        def labels(self, **kwargs):
            raise ValueError("metric not initialised")

    monkeypatch.setattr(metrics, "source_columns_excluded_total", _Boom())
    table = load_table(source_catalog, "events")
    assert replicated_column_names(table) == ("event_id", "properties")
