"""Tests for the demo-metrics population helper."""

from monitoring.cli import populate_demo_metrics
from monitoring.metrics.registry import (
    ml_dataset_build_duration_seconds,
    ml_feature_freshness_seconds,
    ml_prediction_total,
    ml_training_duration_seconds,
)


class TestPopulateDemoMetrics:
    def test_populates_without_error(self):
        populate_demo_metrics()

    def test_dataset_build_duration_recorded(self):
        populate_demo_metrics()
        sample = ml_dataset_build_duration_seconds.labels(
            dataset_name="demo_dataset", environment="local"
        )
        assert sample is not None

    def test_training_duration_recorded(self):
        populate_demo_metrics()
        sample = ml_training_duration_seconds.labels(
            model_name="demo_model", environment="local"
        )
        assert sample is not None

    def test_prediction_count_recorded(self):
        populate_demo_metrics()
        val = ml_prediction_total.labels(
            model_name="demo_model", environment="local"
        )._value.get()
        assert val >= 10

    def test_freshness_recorded(self):
        populate_demo_metrics()
        val = ml_feature_freshness_seconds.labels(
            dataset_name="demo_dataset", environment="local"
        )._value.get()
        assert val == 300.0
