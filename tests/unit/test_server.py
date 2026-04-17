"""Tests for HTTP server health checks."""

from __future__ import annotations

import time

from viaduck.server import _HealthState


def test_health_not_started():
    h = _HealthState()
    assert not h.is_alive()
    assert not h.is_ready()


def test_health_after_start():
    h = _HealthState()
    h.mark_started()
    assert h.is_alive()
    assert h.is_ready()  # ready even without replication (no data yet)


def test_health_poll_recency():
    h = _HealthState(max_poll_age_s=0.01)
    h.mark_started()
    time.sleep(0.02)
    assert not h.is_alive()


def test_health_replication_recency():
    h = _HealthState(max_replication_age_s=0.01)
    h.mark_started()
    h.record_replication()
    time.sleep(0.02)
    assert not h.is_ready()


def test_health_ready_without_replication():
    """If no replication has happened yet, should still be ready (idle source)."""
    h = _HealthState()
    h.mark_started()
    assert h.is_ready()


def test_health_status_body_before_start():
    h = _HealthState()
    body = h.status_body()
    assert "never" in body


def test_health_status_body_after_activity():
    h = _HealthState()
    h.mark_started()
    h.record_poll()
    h.record_replication()
    body = h.status_body()
    assert "ago" in body


def test_health_record_poll_updates_liveness():
    h = _HealthState(max_poll_age_s=0.05)
    h.mark_started()
    time.sleep(0.03)
    h.record_poll()  # refresh
    assert h.is_alive()
