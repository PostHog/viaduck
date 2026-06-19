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
    assert hasattr(metrics.cdc_read_seconds, "observe")
    assert hasattr(metrics.source_snapshot_id, "set")

    # Multi-label metrics should have .labels() that auto-injects pipeline
    assert hasattr(metrics.dest_write_seconds, "labels")
    assert hasattr(metrics.errors_total, "labels")


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
