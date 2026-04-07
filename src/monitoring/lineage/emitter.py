"""Lineage event emitter — builds and persists lineage events as NDJSON."""

import json
from datetime import datetime, timezone
from pathlib import Path

from monitoring.config import LINEAGE_LOG_DIR
from monitoring.contracts.dataset import DatasetMetadata
from monitoring.contracts.training import TrainingMetadata
from monitoring.lineage.schemas import ArtifactRef, LineageEvent


def build_dataset_lineage_event(meta: DatasetMetadata) -> LineageEvent:
    """Build a lineage event from a dataset build metadata contract."""
    return LineageEvent(
        event_type="dataset_build_completed",
        event_time=meta.built_at,
        outputs={
            "dataset": ArtifactRef(
                name=meta.dataset_name,
                version=meta.dataset_version,
            ),
        },
        metadata={
            "row_count": meta.row_count,
            "feature_count": meta.feature_count,
        },
    )


def build_training_lineage_event(meta: TrainingMetadata) -> LineageEvent:
    """Build a lineage event from a training run metadata contract."""
    duration = (meta.completed_at - meta.started_at).total_seconds()
    return LineageEvent(
        event_type="training_run_completed",
        event_time=meta.completed_at,
        inputs={
            "dataset": ArtifactRef(
                name=meta.input_dataset_name,
                version=meta.input_dataset_version,
            ),
        },
        outputs={
            "model": ArtifactRef(
                name=meta.model_name,
                version=meta.model_version,
            ),
        },
        metadata={
            "run_id": meta.run_id,
            "duration_seconds": duration,
        },
    )


def persist_event(event: LineageEvent, log_dir: Path = LINEAGE_LOG_DIR) -> Path:
    """Append a lineage event as a single JSON line to the daily log file."""
    log_dir.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    log_file = log_dir / f"lineage-{date_str}.ndjson"
    line = event.model_dump_json() + "\n"
    with open(log_file, "a") as f:
        f.write(line)
    return log_file


def load_dataset_metadata(path: Path) -> DatasetMetadata:
    """Load and validate dataset metadata from a JSON file."""
    return DatasetMetadata.model_validate_json(path.read_text())


def load_training_metadata(path: Path) -> TrainingMetadata:
    """Load and validate training metadata from a JSON file."""
    return TrainingMetadata.model_validate_json(path.read_text())
