"""Tests for metadata contract parsing."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from monitoring.contracts.dataset import DatasetMetadata
from monitoring.contracts.training import TrainingMetadata

FIXTURES = Path(__file__).parent / "fixtures"


class TestDatasetMetadata:
    def test_parse_valid(self):
        raw = (FIXTURES / "dataset_metadata.json").read_text()
        meta = DatasetMetadata.model_validate_json(raw)
        assert meta.dataset_name == "bike_demand_pti"
        assert meta.row_count == 1245231
        assert meta.feature_count == 38
        assert meta.schema_version == "v1"

    def test_reject_missing_required_field(self):
        raw = (FIXTURES / "dataset_metadata_missing_built_at.json").read_text()
        with pytest.raises(ValidationError):
            DatasetMetadata.model_validate_json(raw)


class TestTrainingMetadata:
    def test_parse_valid(self):
        raw = (FIXTURES / "training_metadata.json").read_text()
        meta = TrainingMetadata.model_validate_json(raw)
        assert meta.run_id == "run_2026_04_07_001"
        assert meta.model_name == "bike_demand_model"
        assert meta.metrics.rmse == pytest.approx(0.182)
        assert meta.metrics.mae == pytest.approx(0.11)

    def test_duration_computable(self):
        raw = (FIXTURES / "training_metadata.json").read_text()
        meta = TrainingMetadata.model_validate_json(raw)
        duration = (meta.completed_at - meta.started_at).total_seconds()
        assert duration == 150.0
