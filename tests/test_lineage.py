"""Tests for lineage event generation and persistence."""

import json
from pathlib import Path

from monitoring.contracts.dataset import DatasetMetadata
from monitoring.contracts.training import TrainingMetadata
from monitoring.lineage.emitter import (
    build_dataset_lineage_event,
    build_training_lineage_event,
    persist_event,
)

FIXTURES = Path(__file__).parent / "fixtures"


class TestDatasetLineageEvent:
    def test_event_type(self):
        meta = DatasetMetadata.model_validate_json(
            (FIXTURES / "dataset_metadata.json").read_text()
        )
        event = build_dataset_lineage_event(meta)
        assert event.event_type == "dataset_build_completed"

    def test_outputs_contain_dataset_ref(self):
        meta = DatasetMetadata.model_validate_json(
            (FIXTURES / "dataset_metadata.json").read_text()
        )
        event = build_dataset_lineage_event(meta)
        assert event.outputs is not None
        assert event.outputs["dataset"].name == "bike_demand_pti"
        assert event.outputs["dataset"].version == "2026-04-07T08:00:00Z"

    def test_metadata_fields(self):
        meta = DatasetMetadata.model_validate_json(
            (FIXTURES / "dataset_metadata.json").read_text()
        )
        event = build_dataset_lineage_event(meta)
        assert event.metadata["row_count"] == 1245231
        assert event.metadata["feature_count"] == 38


class TestTrainingLineageEvent:
    def test_event_type(self):
        meta = TrainingMetadata.model_validate_json(
            (FIXTURES / "training_metadata.json").read_text()
        )
        event = build_training_lineage_event(meta)
        assert event.event_type == "training_run_completed"

    def test_inputs_and_outputs(self):
        meta = TrainingMetadata.model_validate_json(
            (FIXTURES / "training_metadata.json").read_text()
        )
        event = build_training_lineage_event(meta)
        assert event.inputs is not None
        assert event.inputs["dataset"].name == "bike_demand_pti"
        assert event.outputs is not None
        assert event.outputs["model"].name == "bike_demand_model"
        assert event.outputs["model"].version == "v1"

    def test_duration_in_metadata(self):
        meta = TrainingMetadata.model_validate_json(
            (FIXTURES / "training_metadata.json").read_text()
        )
        event = build_training_lineage_event(meta)
        assert event.metadata["duration_seconds"] == 150.0

    def test_run_id_in_metadata(self):
        meta = TrainingMetadata.model_validate_json(
            (FIXTURES / "training_metadata.json").read_text()
        )
        event = build_training_lineage_event(meta)
        assert event.metadata["run_id"] == "run_2026_04_07_001"


class TestPersistEvent:
    def test_persist_creates_ndjson(self, tmp_path: Path):
        meta = DatasetMetadata.model_validate_json(
            (FIXTURES / "dataset_metadata.json").read_text()
        )
        event = build_dataset_lineage_event(meta)
        log_file = persist_event(event, log_dir=tmp_path)

        assert log_file.exists()
        lines = log_file.read_text().strip().split("\n")
        assert len(lines) == 1
        parsed = json.loads(lines[0])
        assert parsed["event_type"] == "dataset_build_completed"

    def test_persist_appends(self, tmp_path: Path):
        meta = DatasetMetadata.model_validate_json(
            (FIXTURES / "dataset_metadata.json").read_text()
        )
        event = build_dataset_lineage_event(meta)
        persist_event(event, log_dir=tmp_path)
        persist_event(event, log_dir=tmp_path)

        log_files = list(tmp_path.glob("*.ndjson"))
        assert len(log_files) == 1
        lines = log_files[0].read_text().strip().split("\n")
        assert len(lines) == 2
