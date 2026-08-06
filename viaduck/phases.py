"""Per-flush phase timing.

One `FlushPhases` object is created per flush submission and threaded down
the whole write path (delivery -> apply -> destination pool). Every phase is
measured with `time.monotonic()` and accumulated by name; on completion the
delivery worker emits them to Prometheus and renders them into the "Flushed
..." log line.

WHY: team-2 flushes of identical shape (~60k rows) are bimodal — ~15s or
80-200s (2026-08-06) — and the flush duration is a single opaque number, so
a slow flush is indistinguishable from a fast one in every signal we have.
Phase attribution turns "this flush was slow" into "this flush spent 62s in
the catalog probe", which is actionable without log-diving or a repro.

Two classes of phase:

- PARTITION phases tile the flush timeline and are meant to be summed:
  `queue_wait` (submit -> worker pickup, OUTSIDE the reported duration),
  then `acquire`, `probe`, `projection`, `append`, `retry_backoff` and
  `cursor_persist`, which tile the reported duration itself.
- NESTED phases subdivide a partition phase and MUST NOT be added to the
  top-level sum: `cold_attach` is the part of `acquire` that built a new
  connection.

`append` is NOT subdivided. pyducklake's Table.append() is one
`INSERT INTO ... SELECT`, so parquet encode, the object-store PUT and the
catalog commit are a single opaque call — see the `probe` phase in
apply.py for how the catalog-vs-storage question is answered instead.

Accumulation is additive across write-retry attempts — a flush that burned
five attempts reports the SUM of its five appends, with the backoff sleeps
between them under `retry_backoff`. Nothing here is thread-safe by design:
a `FlushPhases` belongs to exactly one flush, and the delivery layer's
in-flight guard gives each destination at most one flush worker at a time.
"""

from __future__ import annotations

import time
from contextlib import contextmanager

from viaduck import metrics

# Phases that tile the flush timeline. Order is the log-line render order.
QUEUE_WAIT = "queue_wait"
ACQUIRE = "acquire"
PROBE = "probe"
PROJECTION = "projection"
APPEND = "append"
RETRY_BACKOFF = "retry_backoff"
CURSOR_PERSIST = "cursor_persist"

# Nested phase — a subset of `acquire`, never summed with it.
COLD_ATTACH = "cold_attach"

PARTITION_PHASES = (
    QUEUE_WAIT,
    ACQUIRE,
    PROBE,
    PROJECTION,
    APPEND,
    RETRY_BACKOFF,
    CURSOR_PERSIST,
)
NESTED_PHASES = (COLD_ATTACH,)

# Phases the log line always renders, so a "Flushed ..." line has a stable
# minimum shape. The rest appear only when they carry signal (a disabled
# probe, an identity projection and a retry-free flush would otherwise add
# three permanent `=0.0` fields to every line).
_ALWAYS_RENDERED = (QUEUE_WAIT, ACQUIRE, APPEND, CURSOR_PERSIST)


class FlushPhases:
    """Mutable per-flush accumulator of phase durations, in seconds."""

    __slots__ = ("_d", "_submitted_at", "probe_enabled", "attempts", "cold_attach")

    def __init__(self, submitted_at: float | None = None, *, probe_enabled: bool = False):
        self._d: dict[str, float] = {}
        self._submitted_at = submitted_at
        # Config carried on the object rather than re-plumbed through every
        # call: apply.py and destination.py both need it and neither holds a
        # DeliveryConfig.
        self.probe_enabled = probe_enabled
        # Write-retry attempts actually used (1 on a first-attempt success).
        self.attempts = 0
        self.cold_attach = False

    def start(self) -> None:
        """Called by the flush worker at pickup: closes out `queue_wait`.

        Separate from __init__ because the submitting thread creates the
        object and the worker thread starts the clock — that gap IS the
        measurement.
        """
        if self._submitted_at is not None:
            self.add(QUEUE_WAIT, max(0.0, time.monotonic() - self._submitted_at))

    def add(self, phase: str, seconds: float) -> None:
        self._d[phase] = self._d.get(phase, 0.0) + seconds

    def get(self, phase: str) -> float:
        return self._d.get(phase, 0.0)

    def recorded(self) -> dict[str, float]:
        return dict(self._d)

    @contextmanager
    def time(self, phase: str):
        t0 = time.monotonic()
        try:
            yield
        finally:
            self.add(phase, time.monotonic() - t0)

    def accounted(self) -> float:
        """Sum of the partition phases INSIDE the reported flush duration.

        Excludes `queue_wait`, which happens before the duration clock
        starts, and excludes the nested phases, which would double-count.
        """
        return sum(self._d.get(p, 0.0) for p in PARTITION_PHASES if p != QUEUE_WAIT)

    def observe(self, destination: str) -> None:
        """Emit every recorded phase to Prometheus."""
        for phase, seconds in self._d.items():
            metrics.flush_phase_seconds.labels(destination=destination, phase=phase).observe(seconds)
        if self.attempts:
            metrics.flush_retry_attempts_total.labels(destination=destination).inc(self.attempts)

    def log_fragment(self) -> str:
        """Render the phase breakdown appended to the "Flushed ..." line.

        e.g. `queue=0.1 acquire=0.0 probe=62.3 append=18.9 cursor=0.2`.
        """
        parts: list[str] = []
        # With the probe on, render it even at 0.0: "the probe ran and was
        # instant" is the answer that rules the catalog out, and a missing
        # field would read as "the probe wasn't enabled".
        always = _ALWAYS_RENDERED + (PROBE,) if self.probe_enabled else _ALWAYS_RENDERED
        for phase, label in (
            (QUEUE_WAIT, "queue"),
            (ACQUIRE, "acquire"),
            (PROBE, "probe"),
            (PROJECTION, "projection"),
            (APPEND, "append"),
            (RETRY_BACKOFF, "backoff"),
            (CURSOR_PERSIST, "cursor"),
        ):
            if phase == PROBE and not self.probe_enabled:
                continue
            value = self._d.get(phase)
            if value is None and phase not in always:
                continue
            rendered = f"{label}={value or 0.0:.1f}"
            if phase == ACQUIRE and self.cold_attach:
                rendered += "(cold)"
            parts.append(rendered)
        if self.attempts > 1:
            parts.append(f"attempts={self.attempts}")
        return " ".join(parts)
