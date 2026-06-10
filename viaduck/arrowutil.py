"""Small Arrow construction helpers for hot paths.

These exist because ``pa.array(range(n))`` and ``pa.array([v] * n)``
iterate n Python objects under the GIL — on multi-million-row CDC
batches that's exactly the serial bottleneck the vectorized phases are
meant to avoid (and, post-threading, GIL-held time steals parallelism
from every worker). PyArrow has no ``arange``/``full`` kernels, so we
compose them from C kernels: ``pa.nulls`` allocates without Python
iteration, ``fill_null`` broadcasts a scalar, ``cumulative_sum`` makes
the sequence.
"""

from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc


def row_indices(n: int) -> pa.Array:
    """0-based int64 index array [0, n) built entirely by C kernels."""
    ones = pc.fill_null(pa.nulls(n, pa.int64()), 1)
    return pc.subtract(pc.cumulative_sum(ones), pa.scalar(1, pa.int64()))


def full_bool(n: int, value: bool) -> pa.Array:
    """Boolean array of length n with every slot set to ``value``."""
    return pc.fill_null(pa.nulls(n, pa.bool_()), value)
