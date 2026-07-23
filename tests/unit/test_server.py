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


def test_health_ready_without_replication():
    """An idle source (no writes ever) should still be ready."""
    h = _HealthState()
    h.mark_started()
    assert h.is_ready()


def test_health_ready_with_stale_replication(monkeypatch):
    """Long-stale `_last_replication` must not flip the pod to NotReady; an
    idle source is normal and readiness is gated only on poll recency."""
    clock = {"t": 0.0}
    monkeypatch.setattr("viaduck.server.time.monotonic", lambda: clock["t"])

    h = _HealthState(max_poll_age_s=10)
    h.mark_started()
    h.record_replication()
    clock["t"] = 100_000.0  # ~28h later — far past any plausible gate
    h.record_poll()  # poll loop is still turning
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


def test_lifecycle_endpoint_payload():
    import json as _json

    from viaduck import server as srv

    srv.set_lifecycle_states(
        {"d1": "paused", "d2": "active"},
        rows={
            "d1": {
                "state": "paused",
                "reason": "ops hold",
                "updated_by": "jakob",
                "updated_at": "2026-07-23T00:00:00+00:00",
            }
        },
    )
    payload = _json.loads(srv._lifecycle_json())
    assert payload["destinations"]["d1"]["state"] == "paused"
    assert payload["destinations"]["d1"]["reason"] == "ops hold"
    assert payload["destinations"]["d1"]["updated_by"] == "jakob"
    # d2 has no row (absent = active): state still served, metadata null.
    assert payload["destinations"]["d2"]["state"] == "active"
    assert payload["destinations"]["d2"]["reason"] is None
    # Staleness signal for keep-last-known operation.
    assert payload["loaded_at_age_s"] is not None and payload["loaded_at_age_s"] >= 0
