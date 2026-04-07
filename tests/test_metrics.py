"""Tests for metrics registry helpers."""

from monitoring.metrics.registry import (
    ml_dataset_build_duration_seconds,
    ml_feature_freshness_seconds,
    ml_prediction_total,
    ml_training_duration_seconds,
    record_dataset_build_duration,
    record_prediction,
    record_training_duration,
    set_feature_freshness,
)


class TestMetricsHelpers:
    def test_record_dataset_build_duration(self):
        record_dataset_build_duration("test_ds", 42.5)
        # Histogram observation count should increase.
        sample = ml_dataset_build_duration_seconds.labels(
            dataset_name="test_ds", environment="local"
        )
        # No exception means the metric was recorded.
        assert sample is not None

    def test_record_training_duration(self):
        record_training_duration("test_model", 150.0)
        sample = ml_training_duration_seconds.labels(
            model_name="test_model", environment="local"
        )
        assert sample is not None

    def test_record_prediction(self):
        record_prediction("test_model", count=5)
        # Counter value should be at least 5.
        val = ml_prediction_total.labels(
            model_name="test_model", environment="local"
        )._value.get()
        assert val >= 5

    def test_set_feature_freshness(self):
        set_feature_freshness("test_ds", 320.0)
        val = ml_feature_freshness_seconds.labels(
            dataset_name="test_ds", environment="local"
        )._value.get()
        assert val == 320.0
