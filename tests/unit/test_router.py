"""Tests for Arrow table routing/splitting by field value."""

from __future__ import annotations

import pyarrow as pa
import pytest

from viaduck.config import RoutingConfig
from viaduck.router import Router, RoutingError


@pytest.fixture()
def router() -> Router:
    return Router(RoutingConfig(field="company"))


@pytest.fixture()
def sample_table() -> pa.Table:
    return pa.table(
        {
            "company": ["quacksworth", "mallardine", "quacksworth", "tealford", "mallardine"],
            "value": [1, 2, 3, 4, 5],
        }
    )


@pytest.fixture()
def int_table() -> pa.Table:
    return pa.table(
        {
            "team_id": pa.array([123, 456, 123, 789, 456], type=pa.int64()),
            "value": [1, 2, 3, 4, 5],
        }
    )


# --- String routing ---


def test_split_string_basic(router: Router, sample_table: pa.Table):
    routed, unrouted = router.split_and_count(sample_table, ["quacksworth", "mallardine"])
    assert set(routed.keys()) == {"quacksworth", "mallardine"}
    assert routed["quacksworth"].num_rows == 2
    assert routed["mallardine"].num_rows == 2
    assert unrouted == 1  # tealford


def test_split_string_single_value(router: Router, sample_table: pa.Table):
    routed, _ = router.split_and_count(sample_table, ["tealford"])
    assert routed["tealford"].num_rows == 1


def test_split_string_no_match(router: Router, sample_table: pa.Table):
    routed, unrouted = router.split_and_count(sample_table, ["nonexistent"])
    assert len(routed) == 0
    assert unrouted == 5


def test_split_string_preserves_columns(router: Router, sample_table: pa.Table):
    routed, _ = router.split_and_count(sample_table, ["quacksworth"])
    assert routed["quacksworth"].column_names == ["company", "value"]


# --- Integer routing ---


def test_split_integer_basic(int_table: pa.Table):
    router = Router(RoutingConfig(field="team_id"))
    routed, unrouted = router.split_and_count(int_table, ["123", "456"])
    assert routed["123"].num_rows == 2
    assert routed["456"].num_rows == 2
    assert unrouted == 1  # 789


def test_split_integer_auto_detects_type(int_table: pa.Table):
    """Router should auto-detect integer column and cast routing values."""
    router = Router(RoutingConfig(field="team_id"))
    routed, _ = router.split_and_count(int_table, ["123"])
    assert routed["123"].num_rows == 2


# --- Filter expression building (C5: SQL injection safe via pyducklake.In) ---


def test_build_filter_expr_strings(router: Router):
    expr = router.build_filter_expr(["quacksworth", "mallardine"])
    assert "quacksworth" in expr
    assert "mallardine" in expr
    assert "IN" in expr


def test_build_filter_expr_empty(router: Router):
    expr = router.build_filter_expr([])
    assert expr == "1=0"


def test_build_filter_expr_single(router: Router):
    expr = router.build_filter_expr(["quacksworth"])
    assert "quacksworth" in expr


def test_build_filter_expr_sql_injection_safe(router: Router):
    """Single quotes in routing values must be escaped (C5)."""
    expr = router.build_filter_expr(["O'Brien", "normal"])
    # pyducklake's In expression escapes single quotes as ''
    assert "O''Brien" in expr


# --- Missing routing field (C4) ---


def test_split_missing_field_raises_routing_error(router: Router):
    table = pa.table({"other_column": [1, 2, 3]})
    with pytest.raises(RoutingError, match="company.*not found"):
        router.split_and_count(table, ["quacksworth"])


# --- Null values in routing column (H5) ---


def test_split_null_values_not_routed(router: Router):
    table = pa.table(
        {
            "company": ["quacksworth", None, "mallardine", None],
            "value": [1, 2, 3, 4],
        }
    )
    routed, unrouted = router.split_and_count(table, ["quacksworth", "mallardine"])
    assert routed["quacksworth"].num_rows == 1
    assert routed["mallardine"].num_rows == 1
    assert unrouted == 2  # both NULLs


def test_split_all_null_routing_column(router: Router):
    table = pa.table(
        {
            "company": pa.array([None, None, None], type=pa.string()),
            "value": [1, 2, 3],
        }
    )
    routed, unrouted = router.split_and_count(table, ["quacksworth"])
    assert len(routed) == 0
    assert unrouted == 3


# --- Empty table ---


def test_split_empty_table(router: Router):
    table = pa.table({"company": pa.array([], type=pa.string()), "value": pa.array([], type=pa.int64())})
    routed, unrouted = router.split_and_count(table, ["quacksworth"])
    assert len(routed) == 0
    assert unrouted == 0


# --- All routed ---


def test_split_all_routed(router: Router, sample_table: pa.Table):
    routed, unrouted = router.split_and_count(sample_table, ["quacksworth", "mallardine", "tealford"])
    assert unrouted == 0
    assert sum(t.num_rows for t in routed.values()) == 5


# --- Type mismatch: routing value vs column type (N1) ---


def test_split_invalid_integer_routing_value(int_table: pa.Table):
    """Non-numeric routing value for integer column should raise RoutingError."""
    router = Router(RoutingConfig(field="team_id"))
    with pytest.raises(RoutingError, match="Cannot convert"):
        router.split_and_count(int_table, ["not_a_number"])


def test_split_invalid_float_routing_value():
    """Non-numeric routing value for float column should raise RoutingError."""
    table = pa.table({"score": pa.array([1.5, 2.5], type=pa.float64())})
    router = Router(RoutingConfig(field="score"))
    with pytest.raises(RoutingError, match="Cannot convert"):
        router.split_and_count(table, ["not_a_float"])


# --- Float column routing (N3) ---


def test_split_float_column():
    table = pa.table(
        {
            "score": pa.array([0.5, 1.5, 0.5, 2.5], type=pa.float64()),
            "value": [1, 2, 3, 4],
        }
    )
    router = Router(RoutingConfig(field="score"))
    routed, unrouted = router.split_and_count(table, ["0.5", "1.5"])
    assert routed["0.5"].num_rows == 2
    assert routed["1.5"].num_rows == 1
    assert unrouted == 1


# --- Unicode routing values (N5) ---


def test_split_unicode_routing_values(router: Router):
    table = pa.table({"company": ["café", "北京", "normal"], "value": [1, 2, 3]})
    routed, unrouted = router.split_and_count(table, ["café", "北京"])
    assert routed["café"].num_rows == 1
    assert routed["北京"].num_rows == 1
    assert unrouted == 1


def test_build_filter_expr_unicode(router: Router):
    expr = router.build_filter_expr(["café", "北京"])
    assert "café" in expr
    assert "北京" in expr
