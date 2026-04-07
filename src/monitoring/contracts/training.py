"""Training run metadata contract — P4 → P6."""

from datetime import datetime

from pydantic import BaseModel


class TrainingMetrics(BaseModel):
    """Metrics produced by a training run."""

    rmse: float
    mae: float


class TrainingMetadata(BaseModel):
    """Schema for the metadata JSON emitted by P4 after a training run."""

    run_id: str
    model_name: str
    model_version: str
    input_dataset_name: str
    input_dataset_version: str  # ISO-8601 timestamp string
    started_at: datetime
    completed_at: datetime
    metrics: TrainingMetrics
    artifact_path: str
