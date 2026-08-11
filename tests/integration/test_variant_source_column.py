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
    # Same shape pyducklake's own loader produces.
    upstream = source_catalog.load_table("plain")
    assert [f.name for f in upstream.schema.fields] == list(replicated_column_names(table))
