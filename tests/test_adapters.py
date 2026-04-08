"""Tests for P2/P4 contract adapters."""

from pathlib import Path

import pytest

from monitoring.contracts.adapters import adapt_p4_bundle

FIXTURES = Path(__file__).parent / "fixtures"


class TestAdaptP4Bundle:
    def test_maps_run_id(self):
        meta = adapt_p4_bundle(FIXTURES / "p4_bundle")
        assert meta.run_id == "a1b2c3d4e5f6"

    def test_maps_candidate_name_to_model_name(self):
        meta = adapt_p4_bundle(FIXTURES / "p4_bundle")
        assert meta.model_name == "lightgbm_default"

    def test_model_version_defaults_to_unknown(self):
        """P4 fixture has no model_version field — adapter falls back to 'unknown'."""
        meta = adapt_p4_bundle(FIXTURES / "p4_bundle")
        assert meta.model_version == "unknown"

    def test_uses_test_metrics(self):
        meta = adapt_p4_bundle(FIXTURES / "p4_bundle")
        assert meta.metrics.rmse == pytest.approx(0.182)
        assert meta.metrics.mae == pytest.approx(0.11)

    def test_reads_artifact_path_from_model_uri(self):
        meta = adapt_p4_bundle(FIXTURES / "p4_bundle")
        assert meta.artifact_path == "runs:/a1b2c3d4e5f6/model"

    def test_unknown_dataset_fields(self):
        """P4 does not emit input dataset info — adapter sets 'unknown'."""
        meta = adapt_p4_bundle(FIXTURES / "p4_bundle")
        assert meta.input_dataset_name == "unknown"
        assert meta.input_dataset_version == "unknown"

    def test_duration_zero_when_no_timing(self):
        """P4 only has created_at — started_at == completed_at → duration 0."""
        meta = adapt_p4_bundle(FIXTURES / "p4_bundle")
        assert (meta.completed_at - meta.started_at).total_seconds() == 0.0
