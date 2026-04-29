"""Route Arrow tables by a field value to per-destination batches."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.compute as pc

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
        """
        if self.field not in table.column_names:
            raise RoutingError(
                f"Routing field {self.field!r} not found in table. Available columns: {table.column_names}"
            )

        result: dict[str, pa.Table] = {}
        column = table.column(self.field)
        total_routed = 0

        for val in routing_values:
            scalar = self._make_scalar(val, column.type)
            mask = pc.equal(column, scalar)
            filtered = table.filter(mask)
            if filtered.num_rows > 0:
                result[val] = filtered
                total_routed += filtered.num_rows

        unrouted = table.num_rows - total_routed
        return result, unrouted

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
