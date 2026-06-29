"""Tests for the shared partition-transform module that bridges config
validation (string allowlist) and destination application (pyducklake
Transform sentinels)."""

from __future__ import annotations


def test_transform_names_is_immutable_tuple():
    """`TRANSFORM_NAMES` must be a tuple (immutable) so accidental mutation
    elsewhere doesn't quietly widen the allowlist."""
    from viaduck.partition_transforms import TRANSFORM_NAMES

    assert isinstance(TRANSFORM_NAMES, tuple)


def test_transform_names_excludes_empty_string():
    """Empty string is the parser's sentinel for identity (bare column
    name, no `func(col)` wrapping). It is deliberately NOT in
    TRANSFORM_NAMES because that list gates the `func(col)` syntax only."""
    from viaduck.partition_transforms import TRANSFORM_NAMES

    assert "" not in TRANSFORM_NAMES


def test_transform_map_includes_identity_under_empty_key():
    """The apply-side map uses "" → IDENTITY so destination.py can do a
    single dict lookup regardless of whether the parser saw `col` or
    `func(col)`."""
    from pyducklake.partitioning import IDENTITY

    from viaduck.partition_transforms import get_transform_map

    assert get_transform_map()[""] is IDENTITY


def test_transform_map_keys_match_names_plus_identity():
    """The two halves of the module — `TRANSFORM_NAMES` (config-side) and
    `get_transform_map()` (apply-side) — must agree. The map has all
    TRANSFORM_NAMES entries plus the identity ("") entry; if pyducklake
    grows a new transform we add it to both pieces in one edit here."""
    from viaduck.partition_transforms import TRANSFORM_NAMES, get_transform_map

    map_keys = set(get_transform_map().keys())
    assert map_keys == set(TRANSFORM_NAMES) | {""}


def test_transform_map_resolves_to_pyducklake_sentinels():
    """Each named entry resolves to the corresponding pyducklake Transform
    singleton — verifies we're not silently importing the wrong symbol."""
    from pyducklake.partitioning import DAY, HOUR, MONTH, YEAR

    from viaduck.partition_transforms import get_transform_map

    m = get_transform_map()
    assert m["year"] is YEAR
    assert m["month"] is MONTH
    assert m["day"] is DAY
    assert m["hour"] is HOUR
