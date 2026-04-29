"""Integration tests that exercise the real pyducklake API surface viaduck
relies on — specifically attribute access (property vs method) which
MagicMock-based unit tests cannot validate.

These caught (and now prevent) the production bug where viaduck called
`Table.schema` as a method (`tbl.schema()`) instead of accessing it as a
property. MagicMock tests passed because every mock attribute is callable;
real pyducklake raised `TypeError: 'Schema' object is not callable` at
runtime.

Uses local DuckDB-file catalogs — no Postgres, no Docker — so they run in
the standard `pytest tests/integration/` flow.
"""

from __future__ import annotations

import os

import pytest
from pyducklake import Catalog, Schema
from pyducklake.types import IntegerType, NestedField, StringType

pytestmark = pytest.mark.integration

_SCHEMA = Schema(
    NestedField(field_id=1, name="event_id", field_type=IntegerType(), required=True),
    NestedField(field_id=2, name="company", field_type=StringType(), required=True),
)


def _make_catalog_with_table(tmp_path, name: str = "src") -> Catalog:
    base = tmp_path / name
    os.makedirs(base / "data", exist_ok=True)
    catalog = Catalog(name, str(base / "meta.duckdb"), data_path=str(base / "data"))
    catalog.create_namespace_if_not_exists("default")
    catalog.create_table_if_not_exists("default.events", _SCHEMA)
    return catalog


def test_table_schema_is_property_returning_schema(tmp_path):
    """`tbl.schema` (no parens) returns a Schema. Calling `tbl.schema()` raises.

    Direct exercise of the API contract that broke us in production.
    """
    catalog = _make_catalog_with_table(tmp_path)
    try:
        table = catalog.load_table("default.events")
        # Property access — what viaduck does after the fix.
        schema = table.schema
        assert isinstance(schema, Schema)
        # Method call — what viaduck did before the fix.
        with pytest.raises(TypeError):
            table.schema()
    finally:
        catalog.close()


def test_destination_pool_get_source_schema_against_real_catalog(tmp_path, monkeypatch):
    """Exercise `DestinationPool._get_source_schema` end-to-end against a real
    pyducklake catalog. This is the call path that hit the property-vs-method
    bug in production.
    """
    from unittest.mock import MagicMock

    from viaduck import metrics
    from viaduck.destination import DestinationPool

    metrics.init("test")

    src_base = tmp_path / "src"
    os.makedirs(src_base / "data", exist_ok=True)
    src_meta_db = str(src_base / "meta.duckdb")
    src_data = str(src_base / "data")

    seed = Catalog("source", src_meta_db, data_path=src_data)
    seed.create_namespace_if_not_exists("default")
    seed.create_table_if_not_exists("default.events", _SCHEMA)
    seed.close()

    monkeypatch.setenv("SRC_PG", src_meta_db)

    cfg = MagicMock()
    cfg.source.name = "source"
    cfg.source.postgres_uri_env = "SRC_PG"
    cfg.source.postgres_uri = src_meta_db
    cfg.source.data_path = src_data
    cfg.source.table = "default.events"
    cfg.source.resolved_properties.return_value = {}

    pool = DestinationPool(cfg, max_open=50)
    pool._source_schema = None

    schema = pool._get_source_schema()
    assert isinstance(schema, Schema)
    # Cache populated.
    assert pool._source_schema is schema
