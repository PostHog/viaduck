"""Tests for Prometheus metric definitions and pipeline binding."""

from __future__ import annotations

from unittest.mock import MagicMock

from viaduck.metrics import _AutoPipelineLabels, init


def test_auto_pipeline_labels_injects_pipeline():
    mock_metric = MagicMock()
    wrapped = _AutoPipelineLabels(mock_metric, "my-pipeline")
    wrapped.labels(destination="team-123")
    mock_metric.labels.assert_called_once_with(pipeline="my-pipeline", destination="team-123")


def test_init_binds_pipeline():
    """After init(), module-level metrics should be bound to the pipeline."""
    from viaduck import metrics

    init("test-pipeline")

    # Pre-labeled metrics should have .inc()/.set()/.observe() directly
    assert hasattr(metrics.polls_total, "inc")
    assert hasattr(metrics.poll_cycle_seconds, "observe")
    assert hasattr(metrics.poll_cursor_groups_deferred_total, "inc")
    assert hasattr(metrics.cdc_read_seconds, "observe")
    assert hasattr(metrics.cdc_read_phase_seconds, "labels")
    assert hasattr(metrics.source_snapshot_id, "set")

    # Multi-label metrics should have .labels() that auto-injects pipeline
    assert hasattr(metrics.dest_write_seconds, "labels")
    assert hasattr(metrics.errors_total, "labels")
    assert hasattr(metrics.destination_last_cdc_read_timestamp_seconds, "labels")
    assert hasattr(metrics.destination_cdc_chunk_rows, "labels")
    assert hasattr(metrics.delivery_flush_input_bytes, "labels")


def test_init_binds_delete_metrics():
    """After init(), dest_rows_deleted_total should have .labels()."""
    from viaduck import metrics

    init("test-pipeline")

    assert hasattr(metrics.dest_rows_deleted_total, "labels")


def test_init_binds_upsert_metrics():
    """After init(), dest_rows_upserted_total should have .labels()."""
    from viaduck import metrics

    init("test-pipeline")

    assert hasattr(metrics.dest_rows_upserted_total, "labels")


def test_init_binds_cdc_routing_mutations_metric():
    """After init(), cdc_routing_mutations_total should have .inc()."""
    from viaduck import metrics

    init("test-pipeline")

    assert hasattr(metrics.cdc_routing_mutations_total, "inc")


def test_init_binds_cdc_conflicts_resolved_metric():
    """After init(), cdc_conflicts_resolved_total should have .inc()."""
    from viaduck import metrics

    init("test-pipeline")

    assert hasattr(metrics.cdc_conflicts_resolved_total, "inc")


def test_init_binds_cdc_orphaned_preimages_metric():
    """After init(), cdc_orphaned_preimages_total should have .inc()."""
    from viaduck import metrics

    init("test-pipeline")

    assert hasattr(metrics.cdc_orphaned_preimages_total, "inc")


def test_init_binds_flush_phase_metrics():
    """After init(), the phase histogram and attempt counter should have
    .labels() with the pipeline auto-injected."""
    from viaduck import metrics

    init("test-pipeline")

    assert hasattr(metrics.flush_phase_seconds, "labels")
    assert hasattr(metrics.flush_retry_attempts_total, "labels")


def test_flush_phase_buckets_span_the_slow_flush_range():
    """The bimodal team-2 population sits at ~15s and 80-200s; buckets that
    topped out at 10s (the prometheus_client default) would collapse every
    slow flush into +Inf and make the fast-vs-slow comparison impossible."""
    from viaduck.metrics import _LATENCY_BUCKETS

    assert _LATENCY_BUCKETS == (0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0)


def test_remove_destination_series_leaves_nothing_behind():
    """A retired tenant must not keep reporting. Every per-destination series
    this module can name has to be removable — including the phase histogram,
    whose third label means it needs enumerating rather than a bare remove()."""
    from prometheus_client import generate_latest

    from viaduck import metrics

    init("test-pipeline")
    dest = "removal-probe-dest"

    metrics.dest_lag_snapshots.labels(destination=dest).set(1)
    metrics.delivery_buffer_rows.labels(destination=dest).set(1)
    metrics.delivery_flush_input_rows.labels(destination=dest).observe(1)
    metrics.delivery_flush_input_bytes.labels(destination=dest).observe(1)
    metrics.flush_retry_attempts_total.labels(destination=dest).inc()
    metrics.destination_cdc_chunk_rows.labels(destination=dest).observe(1)
    metrics.destination_cdc_chunk_bytes.labels(destination=dest).observe(1)
    metrics.destination_last_cdc_read_timestamp_seconds.labels(destination=dest).set(1)
    for phase in ("queue_wait", "acquire", "resolve", "append", "cursor_persist", "cold_attach"):
        metrics.flush_phase_seconds.labels(destination=dest, phase=phase).observe(1)
    # The two known-unremoved series, observed so the assertion pins the gap
    # exactly rather than checking for an empty set — which would also pass
    # if the removal set silently shrank.
    metrics.dest_write_seconds.labels(destination=dest).observe(1)
    metrics.delivery_flush_seconds.labels(destination=dest).observe(1)

    metrics.remove_destination_series(dest)

    survivors = sorted(
        {
            line.split("{")[0]
            .removesuffix("_bucket")
            .removesuffix("_count")
            .removesuffix("_sum")
            .removesuffix("_created")
            for line in generate_latest().decode().splitlines()
            if f'destination="{dest}"' in line
        }
    )
    # dest_write_seconds / delivery_flush_seconds predate this instrumentation
    # and removing them would drop history dashboards still read, so that is a
    # separate call. Nothing added for phase timing or read-shape
    # observability may join them.
    assert survivors == ["viaduck_delivery_flush_seconds", "viaduck_dest_write_seconds"], survivors


def test_removal_of_a_multi_label_series_fails_loudly_if_mislisted():
    """The removal loop catches KeyError only. A metric with an extra label
    raises ValueError there instead of silently leaking — the guard that
    makes the flush_phase_seconds enumeration necessary rather than optional."""
    import pytest

    from viaduck import metrics

    init("test-pipeline")
    metrics.flush_phase_seconds.labels(destination="d-partial", phase="append").observe(1)
    with pytest.raises(ValueError):
        metrics.flush_phase_seconds.remove("d-partial")
