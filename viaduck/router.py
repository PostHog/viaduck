"""Route Arrow tables by a field value to per-destination batches."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.compute as pc

from viaduck.arrowutil import row_indices

if TYPE_CHECKING:
    from viaduck.config import RoutingConfig

log = logging.getLogger(__name__)


class RoutingError(Exception):
    pass


class Router:
    """Splits an Arrow table by a routing field into per-destination batches."""

    def __init__(self, cfg: RoutingConfig):
        self.field = cfg.field

    def build_filter_expr(self, routing_values: list[str]) -> str:
        """Build a SQL filter expression for CDC pushdown.

        Uses pyducklake's In expression for proper SQL escaping.
        """
        from pyducklake.expressions import In

        if not routing_values:
            return "1=0"
        return In(self.field, tuple(routing_values)).to_sql()

    def split_and_count(self, table: pa.Table, routing_values: list[str]) -> tuple[dict[str, pa.Table], int]:
        """Split Arrow table by routing field and count unrouted rows in one pass.

        Returns (routed_dict, unrouted_count).

        Single pass over the data: one ``index_in`` kernel maps each row to
        its routing-value code (null = unrouted), one stable sort groups the
        row indices by code, and each destination's batch is a contiguous
        ``take``. Replaces the one-filter-per-destination loop, which was
        O(destinations x rows) per group.
        """
        if self.field not in table.column_names:
            raise RoutingError(
                f"Routing field {self.field!r} not found in table. Available columns: {table.column_names}"
            )

        result: dict[str, pa.Table] = {}
        if not routing_values:
            return result, table.num_rows

        column = table.column(self.field)
        value_set = self._typed_value_set(routing_values, column.type)

        # Two configured routing values that collide after type conversion
        # ("1" and "01" on an int column) would silently deliver rows only
        # to the first (index_in returns the first match). The predecessor
        # delivered the same rows to BOTH destinations — also wrong, just
        # quieter. Reject the config instead of picking a wrong behavior.
        distinct = pc.count_distinct(value_set).as_py()
        if distinct != len(value_set):
            raise RoutingError(
                f"Routing values collide after conversion to column type {column.type}: "
                f"{routing_values!r} maps to {value_set.to_pylist()!r}"
            )

        codes = pc.index_in(column, value_set=value_set)
        routed_mask = pc.is_valid(codes)
        total_routed = pc.sum(pc.cast(routed_mask, pa.int64())).as_py() or 0
        unrouted = table.num_rows - total_routed
        if total_routed == 0:
            return result, unrouted

        # Stable sort of routed row indices by code: per-destination rows
        # stay in original order (matching the old filter behavior), and
        # each destination is a contiguous slice of the permutation.
        idx = pa.table({"__code": codes, "__idx": row_indices(table.num_rows)})
        idx = idx.filter(routed_mask).sort_by("__code")
        sorted_codes = idx.column("__code")
        sorted_idx = idx.column("__idx")

        counts = pa.table({"c": sorted_codes}).group_by("c").aggregate([("c", "count")]).sort_by("c")
        offset = 0
        for code, count in zip(counts.column("c").to_pylist(), counts.column("c_count").to_pylist()):
            result[routing_values[code]] = table.take(sorted_idx.slice(offset, count))
            offset += count

        return result, unrouted

    def _typed_value_set(self, values: list[str], column_type: pa.DataType) -> pa.Array:
        """Convert configured routing values (strings) to a typed Arrow array.

        int/float/bool reuse _make_scalar's parsing rules (e.g. "yes"/"t"
        accepted as booleans) — their as_py() round-trip is lossless. All
        other types cast the string array directly with Arrow's cast
        machinery: no Python-object round-trip, so sub-microsecond
        timestamp precision and decimal exactness survive.
        """
        if (
            pa.types.is_integer(column_type)
            or pa.types.is_floating(column_type)
            or pa.types.is_boolean(column_type)
            or pa.types.is_string(column_type)
            or pa.types.is_large_string(column_type)
        ):
            return pa.array(
                [self._make_scalar(val, column_type).as_py() for val in values],
                type=column_type,
            )
        try:
            return pc.cast(pa.array(values, type=pa.string()), column_type)
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError) as exc:
            raise RoutingError(f"Cannot convert routing values {values!r} to column type {column_type}: {exc}") from exc

    def _make_scalar(self, value: str, column_type: pa.DataType) -> pa.Scalar:
        """Create a PyArrow scalar matching the column's actual type.

        Handles every category of column type the routing field might be:
        integer, floating, boolean, string-ish, and any other type that PyArrow
        can cast a string to (decimal, date, timestamp, etc.). Falling back to
        a string scalar for non-numeric types caused
        `pc.equal(<bool|date|...> column, <string scalar>)` to raise
        `ArrowNotImplementedError` at runtime.

        Raises RoutingError if the value cannot be converted to the column type.
        """
        try:
            if pa.types.is_integer(column_type):
                return pa.scalar(int(value), type=column_type)
            if pa.types.is_floating(column_type):
                return pa.scalar(float(value), type=column_type)
            if pa.types.is_boolean(column_type):
                normalized = value.strip().lower()
                if normalized in ("true", "1", "yes", "t", "y"):
                    return pa.scalar(True, type=column_type)
                if normalized in ("false", "0", "no", "f", "n"):
                    return pa.scalar(False, type=column_type)
                raise ValueError(f"cannot interpret {value!r} as boolean")
            if pa.types.is_string(column_type) or pa.types.is_large_string(column_type):
                return pa.scalar(value, type=column_type)
            # Decimal, date, timestamp, time, duration, etc. — let PyArrow's
            # cast machinery parse the string against the column type.
            return pa.scalar(value).cast(column_type)
        except (ValueError, TypeError, pa.ArrowInvalid, pa.ArrowNotImplementedError) as exc:
            raise RoutingError(f"Cannot convert routing value {value!r} to column type {column_type}: {exc}") from exc
