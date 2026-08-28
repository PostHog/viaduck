"""Schema projection from a source Arrow batch to a destination table schema.

viaduck's default write path is schema-preserving: pyducklake's `Table.append`
does `INSERT INTO tbl SELECT * FROM tmp_arrow_batch`, which is positional, so
source and destination schemas have to line up exactly. That works when the
destination table was set up specifically to mirror the source (the current
`events_nrt → events_nrt` topology), but breaks the moment we want to write
into a table someone else defined with its own shape — canonical
``posthog.events`` (backfilled by DLT) being the driving example: 25 columns
vs the source's 26, ``timestamptz`` on 8 columns where the source has
``varchar``, an extra ``captured_at`` in the source that isn't in the target,
and enough column-order shuffling that positional insert would silently
write ``person_properties`` into ``person_created_at``'s slot.

This module builds a deterministic per-batch transformation once at connect
time (from the source and target Arrow schemas, plus a small allow-list for
columns the operator has explicitly said to drop) and applies it before every
write. It fails LOUD at build time if the projection can't be resolved
safely — an unknown source column not in the drop allow-list, or a NOT NULL
target column with no corresponding source. Silent shape drift is exactly
what got us here; the loud failure is a feature.

The cast machinery is Arrow-side (pyarrow.compute). It has one special case
worth calling out: ``varchar → timestamptz`` follows millpond's
``arrow_converter._to_timestamptz`` pattern (cast to naive ``timestamp(us)``
via Arrow's ISO-8601 parser, then ``assume_timezone(UTC)``) because
PyArrow's direct tz-aware cast demands an explicit zone offset in the input
string, which the ClickHouse producer's format doesn't reliably emit.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pc

log = logging.getLogger(__name__)

# Signature: (column_name, num_failed_values) -> None. Called from `apply()`
# when the per-value cast fallback nulls one or more values. Callers (see
# apply.py) bind this to the `projection_cast_null_fallback_total{destination,column}`
# counter so a producer-format drift is loud in metrics, not silent-in-nulls.
NullFallbackCallback = Callable[[str, int], None]


class SchemaProjectionError(Exception):
    """Raised when a source schema cannot be safely projected to a target schema."""


@dataclass(frozen=True)
class ProjectionPlan:
    """Immutable description of a source→target projection.

    Serves as both the executor (via `.apply()`) and an audit log (via
    `.describe()`). Building the plan validates that the projection is
    resolvable; applying it is pure Arrow compute.
    """

    target_schema: pa.Schema
    source_column_order: tuple[str, ...] = ()
    drop_columns: tuple[str, ...] = ()
    null_fill_columns: tuple[str, ...] = ()
    # (column_name, source_type, target_type)
    cast_columns: tuple[tuple[str, pa.DataType, pa.DataType], ...] = ()
    passthrough_columns: tuple[str, ...] = ()

    def apply(
        self,
        batch: pa.Table,
        *,
        on_null_fallback: NullFallbackCallback | None = None,
    ) -> pa.Table:
        """Project `batch` to match `self.target_schema` exactly.

        Applies drops → null-fills → casts → reorder. The output is a new
        `pa.Table` whose schema is byte-for-byte `self.target_schema` at
        the field-set + type level (Arrow schema equality is structural).

        `on_null_fallback(column_name, num_failed)` fires once per column
        that had one or more values nulled by the per-value cast fallback.
        Callers should wire it to the null-fallback metric — silent nulling
        of a producer-format drift is the exact anti-pattern this module
        was written against.
        """
        # 1. Drop columns not represented in target (and pre-approved via drop_columns).
        if self.drop_columns:
            keep = [name for name in batch.column_names if name not in set(self.drop_columns)]
            batch = batch.select(keep)

        # 2. Null-fill columns present in target but absent from source. Validation at
        #    build time already confirmed each of these is nullable in the target.
        for name in self.null_fill_columns:
            target_type = self.target_schema.field(name).type
            batch = batch.append_column(name, pa.nulls(len(batch), type=target_type))

        # 3. Cast columns present in both with mismatched types.
        for name, src_type, dst_type in self.cast_columns:
            idx = batch.schema.get_field_index(name)
            if idx < 0:
                # get_field_index returns -1 for missing/duplicated fields,
                # and Table.column(-1) silently returns the LAST column —
                # guard explicitly or a drifted batch casts the wrong column.
                raise RuntimeError(
                    f"schema_projection: planned cast column {name!r} missing (or duplicated) in batch schema "
                    f"{[f.name for f in batch.schema]} — source schema drifted from the connect-time plan"
                )
            col = batch.column(idx)
            casted, failed = _cast_column(col, src_type, dst_type)
            batch = batch.set_column(idx, name, casted)
            if failed and on_null_fallback is not None:
                on_null_fallback(name, failed)

        # 4. Reorder to target column order. pyducklake's INSERT is positional
        #    (`INSERT INTO tbl SELECT * FROM tmp`), so column order at the write
        #    boundary must match. Even if pyducklake ever switches to BY NAME,
        #    canonicalizing here keeps the boundary predictable.
        target_names = [f.name for f in self.target_schema]
        batch = batch.select(target_names)

        return batch

    def describe(self) -> str:
        """One-line summary for INFO logs at destination-pool open time."""
        parts: list[str] = []
        if self.drop_columns:
            parts.append(f"drop={list(self.drop_columns)}")
        if self.null_fill_columns:
            parts.append(f"null_fill={list(self.null_fill_columns)}")
        if self.cast_columns:
            casts = [f"{n}:{s}→{d}" for n, s, d in self.cast_columns]
            parts.append(f"cast=[{', '.join(casts)}]")
        if not parts:
            return "identity"
        return " ".join(parts)

    @property
    def is_identity(self) -> bool:
        """True when the plan is a pure passthrough AND source order == target order.

        Order matters: pyducklake's `Table.append` is positional. A plan with
        identical field-sets and types but a different column order still needs
        `.apply()` to run for the reorder step at `apply()` step 4. Skipping
        `.apply()` in that case is the silent-slot-corruption bug class this
        module exists to prevent.
        """
        if self.drop_columns or self.null_fill_columns or self.cast_columns:
            return False
        return self.source_column_order == tuple(f.name for f in self.target_schema)


def build(
    source_schema: pa.Schema,
    target_schema: pa.Schema,
    *,
    drop_allow: set[str] | None = None,
    key_columns: tuple[str, ...] = (),
    routing_field: str | None = None,
) -> ProjectionPlan:
    """Build a projection from `source_schema` to `target_schema`.

    Rules:
      - Column in source but not target: must appear in `drop_allow`, else raise.
        Silent drops are the bug class that produced this module in the first
        place; opt-in only.
      - Column in target but not source: null-fill. Target column must be
        nullable, else raise.
      - Column in both, same type: passthrough.
      - Column in both, different types: cast added.

    Identity guards (raise at build time, never at write time):
      - Any column in `key_columns` may NOT be cast, dropped, or null-filled.
        Phase 3's delete filter is built by matching key-column values across
        the insert batch that landed the row and the delete batch that removes
        it; a non-bijective transform on a key column produces a filter that
        fails to match the earlier upsert, leaving a phantom that no future
        delete can remove (violates `NoPhantomWhenCurrent` per the TLA spec).
      - The `routing_field` may not be dropped, cast, or null-filled. Routing
        happens BEFORE projection, so a projected batch whose routing column
        no longer matches `RoutingMap[d]` violates `PartitionCorrectness`.
    """
    drop_allow = drop_allow or set()
    src_names = set(source_schema.names)
    tgt_names = set(target_schema.names)

    drops: list[str] = []
    for name in source_schema.names:
        if name in tgt_names:
            continue
        if name not in drop_allow:
            raise SchemaProjectionError(
                f"Source column {name!r} not present in target schema and not in the "
                f"drop allow-list. Add to destinations[…].schemaProjection.dropSourceColumns "
                f"to acknowledge the drop, or update the target schema to accept it."
            )
        drops.append(name)

    null_fills: list[str] = []
    for name in target_schema.names:
        if name in src_names:
            continue
        field = target_schema.field(name)
        if not field.nullable:
            raise SchemaProjectionError(
                f"Target column {name!r} is NOT NULL but absent from source. Cannot "
                f"null-fill. Either add the column to the source (millpond), make the "
                f"target column nullable, or give the target a default value."
            )
        null_fills.append(name)

    casts: list[tuple[str, pa.DataType, pa.DataType]] = []
    passthroughs: list[str] = []
    for name in target_schema.names:
        if name not in src_names:
            continue  # already recorded as null_fill above
        src_type = source_schema.field(name).type
        dst_type = target_schema.field(name).type
        if src_type == dst_type:
            passthroughs.append(name)
        else:
            casts.append((name, src_type, dst_type))

    # T1 guard: refuse a cast that lands on a NOT NULL target. The per-value
    # cast fallback (`_cast_column_per_value`) nulls unparseable values on
    # producer format drift; those NULLs would then hit the DuckLake NOT NULL
    # constraint at write time and stall the flush indefinitely (retries pull
    # the same window and re-fail identically). Refuse loud at build rather
    # than defer to a runtime stall. See tla/Viaduck.tla docblock's "PROJECTION
    # FAILURE" note — this guard is what makes the liveness rationale hold.
    offending_notnull_casts = sorted(name for name, _, _ in casts if not target_schema.field(name).nullable)
    if offending_notnull_casts:
        raise SchemaProjectionError(
            f"Target columns {offending_notnull_casts!r} are NOT NULL and require a "
            f"type cast from source. Refusing: the per-value cast fallback nulls "
            f"unparseable values, and a NULL would then violate the target's NOT NULL "
            f"constraint at write time — the flush would stall on retries indefinitely. "
            f"Either align the source and target types for these columns, or make the "
            f"target column nullable."
        )

    key_set = set(key_columns)
    if key_set:
        cast_names = {c[0] for c in casts}
        offending_key_casts = sorted(key_set & cast_names)
        if offending_key_casts:
            raise SchemaProjectionError(
                f"Key columns {offending_key_casts!r} require a type cast, which is "
                f"unsafe: Phase 3 delete matching depends on key values round-tripping "
                f"exactly between insert and delete batches, and a cast is not "
                f"guaranteed to be injective (e.g. varchar→bigint on padded strings, "
                f"cross-batch varchar→timestamptz parse drift). Align the source and "
                f"target types for key columns before enabling schema projection."
            )
        offending_key_drops = sorted(key_set & set(drops))
        if offending_key_drops:
            raise SchemaProjectionError(
                f"Key columns {offending_key_drops!r} are in drop_source_columns. "
                f"Refusing: a key column must land on the destination for delete matching."
            )
        offending_key_nulls = sorted(key_set & set(null_fills))
        if offending_key_nulls:
            raise SchemaProjectionError(
                f"Key columns {offending_key_nulls!r} are missing from source and would "
                f"be null-filled. Refusing: a key column with no source value cannot "
                f"identify rows for delete matching."
            )

    if routing_field is not None:
        if routing_field in set(drops):
            raise SchemaProjectionError(
                f"Routing column {routing_field!r} is in drop_source_columns. Refusing: "
                f"the routing column identifies which destination a row landed in and "
                f"must be preserved on the destination row (PartitionCorrectness)."
            )
        if routing_field in {c[0] for c in casts}:
            raise SchemaProjectionError(
                f"Routing column {routing_field!r} requires a type cast. Refusing: "
                f"routing happens before projection using the source value, so a cast "
                f"that changes the value would violate PartitionCorrectness. Align the "
                f"source and target types for the routing column."
            )
        if routing_field in set(null_fills):
            raise SchemaProjectionError(
                f"Routing column {routing_field!r} is missing from source. Refusing: "
                f"a null-filled routing column cannot match RoutingMap[d]."
            )

    return ProjectionPlan(
        target_schema=target_schema,
        source_column_order=tuple(source_schema.names),
        drop_columns=tuple(drops),
        null_fill_columns=tuple(null_fills),
        cast_columns=tuple(casts),
        passthrough_columns=tuple(passthroughs),
    )


def _cast_column(
    col: pa.ChunkedArray,
    src_type: pa.DataType,
    dst_type: pa.DataType,
) -> tuple[pa.ChunkedArray, int]:
    """Cast one column from src_type to dst_type. Returns (casted, num_failed).

    Special case: `varchar → timestamp(tz=UTC)` uses the millpond
    `arrow_converter._to_timestamptz` pattern. PyArrow's direct tz-aware cast
    demands an explicit zone offset in every input string, which the
    ClickHouse producer's format doesn't reliably emit. The naive cast uses
    Arrow's ISO-8601 parser which accepts the space-separator + 0/3/6
    fractional-digit variants ClickHouse emits, and then we stamp UTC via
    `assume_timezone`.

    Arrow's `pc.cast` is all-or-nothing: one unparseable value raises for the
    whole column. That's a load-bearing hazard on a CDC replication path — a
    single bad row in a batch would stall the destination indefinitely
    (retries pull the same window). So we mirror millpond's `_coerce_or_null`:
    on ArrowInvalid, fall back to a per-value pass that nulls only the bad
    values. The per-value pass is cold — it runs only when the fast path
    fails. Callers get `num_failed` to bump a metric so the drift is loud.

    The result is ALWAYS `dst_type`, even on total failure — a mixed-type
    column would break the destination write path in surprising ways
    downstream.
    """
    try:
        return _cast_column_fast(col, src_type, dst_type), 0
    except (pa.ArrowInvalid, pa.ArrowTypeError):
        return _cast_column_per_value(col, src_type, dst_type)


def _cast_column_fast(
    col: pa.ChunkedArray,
    src_type: pa.DataType,
    dst_type: pa.DataType,
) -> pa.ChunkedArray:
    if pa.types.is_string(src_type) and pa.types.is_timestamp(dst_type):
        naive = pc.cast(col, pa.timestamp("us"))
        if dst_type.tz is None:
            return naive
        return pc.assume_timezone(naive, dst_type.tz)
    return pc.cast(col, dst_type)


def _cast_column_per_value(
    col: pa.ChunkedArray,
    src_type: pa.DataType,
    dst_type: pa.DataType,
) -> tuple[pa.ChunkedArray, int]:
    """Slow-path per-value cast: null unparseable values, count them."""
    out: list = []
    failed = 0
    for v in col.to_pylist():
        if v is None:
            out.append(None)
            continue
        try:
            one = _cast_column_fast(pa.array([v], type=src_type), src_type, dst_type)
            out.append(one[0].as_py())
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            out.append(None)
            failed += 1
    return pa.array(out, type=dst_type), failed
