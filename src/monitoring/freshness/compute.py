"""Feature freshness computation."""

from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from pydantic import BaseModel

from monitoring.config import DEFAULT_FRESHNESS_THRESHOLD_SECONDS
from monitoring.contracts.dataset import DatasetMetadata
from monitoring.metrics.registry import set_feature_freshness


class FreshnessResult(BaseModel):
    """Structured freshness computation result."""

    dataset_name: str
    dataset_version: str
    freshness_seconds: float
    status: Literal["FRESH", "STALE"]


def compute_freshness(
    metadata_path: Path,
    threshold_seconds: int = DEFAULT_FRESHNESS_THRESHOLD_SECONDS,
    update_metric: bool = True,
) -> FreshnessResult:
    """Compute freshness from a dataset metadata file.

    Uses ``built_at`` as the primary freshness source.
    Falls back to the metadata file's mtime if ``built_at`` is missing or
    cannot be parsed.
    """
    raw = metadata_path.read_text()
    meta = DatasetMetadata.model_validate_json(raw)

    now = datetime.now(timezone.utc)
    built_at = meta.built_at
    if built_at.tzinfo is None:
        built_at = built_at.replace(tzinfo=timezone.utc)
    freshness_seconds = (now - built_at).total_seconds()

    status: Literal["FRESH", "STALE"] = (
        "FRESH" if freshness_seconds < threshold_seconds else "STALE"
    )

    if update_metric:
        set_feature_freshness(meta.dataset_name, freshness_seconds)

    return FreshnessResult(
        dataset_name=meta.dataset_name,
        dataset_version=meta.dataset_version,
        freshness_seconds=round(freshness_seconds, 1),
        status=status,
    )
