"""Adapters for reading real P2/P4 artifacts into P6 locked contracts.

P2 (mobility-feature-pipeline) does not emit a standalone JSON matching
the P6 DatasetMetadata schema.  Instead it embeds metadata in the Parquet
schema and stores the parquet file under output/.

P4 (ml-training-orchestrator) writes artifacts/<run_id>/metadata.json and
artifacts/<run_id>/metrics.json but with a different schema than the P6
TrainingMetadata contract.

These adapters bridge the gap so P6 can operate in pull-mode against
real upstream artifacts without modifying P2 or P4.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

from monitoring.contracts.dataset import DatasetMetadata
from monitoring.contracts.training import TrainingMetadata, TrainingMetrics

# Columns in P2 parquet that are NOT features.
_NON_FEATURE_COLS = {"station_id", "obs_ts", "feature_cutoff_ts",
                     "label_window_end", "target_empty_next_hour"}


def adapt_p2_parquet(parquet_path: Path) -> DatasetMetadata:
    """Build a DatasetMetadata from a real P2 training-dataset parquet file.

    Reads the Parquet schema metadata embedded by P2's pipeline.py and
    derives any fields not directly present.

    Requires pyarrow.
    """
    import pyarrow.parquet as pq

    schema = pq.read_schema(str(parquet_path))
    raw_meta = {k.decode(): v.decode() for k, v in (schema.metadata or {}).items()}

    # Parse build_timestamp "YYYYMMDD_HHMMSS" → datetime
    ts_str = raw_meta["build_timestamp"]
    built_at = datetime.strptime(ts_str, "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)

    # Feature columns = all parquet columns minus known non-feature columns
    all_cols = {f.name for f in schema}
    feature_cols = all_cols - _NON_FEATURE_COLS
    feature_count = len(feature_cols)

    row_count = int(raw_meta["row_count"])

    # Derive dataset_name from filename: training_dataset_YYYYMMDD_HHMMSS.parquet
    dataset_name = parquet_path.stem  # e.g. "training_dataset_20260403_230612"

    return DatasetMetadata(
        dataset_name=dataset_name,
        dataset_version=built_at.isoformat(),
        path=str(parquet_path),
        built_at=built_at,
        row_count=row_count,
        feature_count=feature_count,
        target="target_empty_next_hour",
        entity="station_id",
        event_ts="obs_ts",
        schema_version="v1",
    )


def adapt_p4_bundle(bundle_dir: Path) -> TrainingMetadata:
    """Build a TrainingMetadata from a real P4 training-bundle directory.

    Reads metadata.json + metrics.json from the bundle and maps fields
    to the P6 contract.  Fields not present in P4 are derived with
    reasonable defaults.
    """
    meta_raw = json.loads((bundle_dir / "metadata.json").read_text())
    metrics_raw = json.loads((bundle_dir / "metrics.json").read_text())

    run_id: str = meta_raw["run_id"]
    candidate_name: str = meta_raw["candidate_name"]
    model_type: str = meta_raw["model_type"]
    created_at_str: str = meta_raw["created_at"]
    created_at = datetime.fromisoformat(created_at_str)

    # P4 metrics use val_/test_ prefixes.  Map test metrics → P6 contract.
    rmse = metrics_raw.get("test_rmse", metrics_raw.get("val_rmse", 0.0))
    mae = metrics_raw.get("test_mae", metrics_raw.get("val_mae", 0.0))

    # model_uri.txt contains e.g. "runs:/<run_id>/model"
    model_uri_path = bundle_dir / "model_uri.txt"
    artifact_path = model_uri_path.read_text().strip() if model_uri_path.exists() else f"runs:/{run_id}/model"

    # Use real input dataset fields and timestamps if P4 emits them,
    # otherwise fall back to defaults for older bundles.
    input_dataset_name = meta_raw.get("input_dataset_name", "unknown")
    input_dataset_version = meta_raw.get("input_dataset_version", "unknown")

    started_at = datetime.fromisoformat(meta_raw["started_at"]) if "started_at" in meta_raw else created_at
    completed_at = datetime.fromisoformat(meta_raw["completed_at"]) if "completed_at" in meta_raw else created_at

    return TrainingMetadata(
        run_id=run_id,
        model_name=candidate_name,
        model_version=meta_raw.get("model_version", "unknown"),
        input_dataset_name=input_dataset_name,
        input_dataset_version=input_dataset_version,
        started_at=started_at,
        completed_at=completed_at,
        metrics=TrainingMetrics(rmse=rmse, mae=mae),
        artifact_path=artifact_path,
    )
