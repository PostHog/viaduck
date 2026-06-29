"""Single source-of-truth for partition transform names + their mappings
to pyducklake Transform sentinels.

config.py consumes `TRANSFORM_NAMES` at YAML-validation time to allow-list
the function names that may appear in `partition_by` entries.

destination.py consumes `get_transform_map()` at apply time to translate
the string names back into the pyducklake Transform constants that
`UpdateSpec.add_field(column, transform)` expects.

Keeping the knowledge in one module ensures that adding a new transform
(e.g. `bucket`, `truncate` when pyducklake supports them) requires a
single edit here, not two coordinated edits across config + destination.
"""

from __future__ import annotations

import functools

# Allowed function names in `partition_by` config entries. The empty string
# is reserved by the parser for bare identifiers (identity transform); it
# is intentionally NOT included here because config.py treats "no parens"
# as a separate branch from "func(col)".
TRANSFORM_NAMES: tuple[str, ...] = ("year", "month", "day", "hour")


@functools.cache
def get_transform_map() -> dict[str, object]:
    """Map from config-string ("" → identity, "year"/"month"/etc) to the
    pyducklake Transform sentinel that `UpdateSpec.add_field` expects.

    `@functools.cache` (thread-safe per CPython docs) memoizes after the
    first call so the pyducklake import + dict construction happens once
    per process. Pyducklake's Transform sentinels are module-level
    singletons (verified by `test_transform_map_resolves_to_pyducklake_sentinels`)
    so caching the dict by identity is safe — multiple callers receive
    the SAME dict object and the same Transform references, which matters
    for the `is`/`==` comparison in destination.py's catch-verify path.

    Lazy import keeps callers that only need names (config validation)
    independent of pyducklake.
    """
    from pyducklake.partitioning import DAY, HOUR, IDENTITY, MONTH, YEAR

    return {"": IDENTITY, "year": YEAR, "month": MONTH, "day": DAY, "hour": HOUR}
