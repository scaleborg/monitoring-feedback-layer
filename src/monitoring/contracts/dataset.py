"""Dataset build metadata contract — P2 → P6."""

from datetime import datetime

from pydantic import BaseModel


class DatasetMetadata(BaseModel):
    """Schema for the metadata JSON emitted by P2 after a dataset build."""

    dataset_name: str
    dataset_version: str  # ISO-8601 timestamp string
    path: str
    built_at: datetime
    row_count: int
    feature_count: int
    target: str
    entity: str
    event_ts: str
    schema_version: str
