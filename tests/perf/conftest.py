"""Perf test infrastructure: collect timing results and optionally emit JSON.

Single-iteration timing with optional JSON output. For CI regression detection,
consider adding --perf-runs for multi-run statistical analysis (mean, stdev, p95).
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path

import pytest

_results: list[dict] = []


@dataclass
class PerfTimer:
    """Context manager that records elapsed time and stores results."""

    name: str
    scale: str
    _start: float = field(default=0, init=False)
    elapsed: float = field(default=0, init=False)

    def __enter__(self):
        self._start = time.monotonic()
        return self

    def __exit__(self, *args):
        self.elapsed = time.monotonic() - self._start
        _results.append({"test": self.name, "scale": self.scale, "elapsed_s": round(self.elapsed, 4)})
        return False


@pytest.fixture()
def perf_timer():
    """Fixture that returns a PerfTimer factory."""

    def _make(name: str, scale: str) -> PerfTimer:
        return PerfTimer(name=name, scale=scale)

    return _make


def pytest_sessionfinish(session, exitstatus):
    """After all tests, optionally write results to JSON (atomic via temp file)."""
    if not _results:
        return
    out = session.config.getoption("--perf-json", default=None)
    if out:
        out_path = Path(out)
        tmp_path = out_path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(_results, indent=2) + "\n")
        tmp_path.rename(out_path)


def pytest_addoption(parser):
    parser.addoption("--perf-json", default=None, help="Path to write perf results JSON")
