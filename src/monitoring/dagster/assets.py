"""Dagster assets wrapping existing P6 monitoring logic."""

from pathlib import Path

from dagster import asset

DATASET_METADATA_PATH = Path("tests/fixtures/dataset_metadata.json")
TRAINING_METADATA_PATH = Path("tests/fixtures/training_metadata.json")


@asset
def validation_result() -> dict:
    """Run validation checks against dataset metadata."""
    from monitoring.validation.checks import run_checks

    report = run_checks(DATASET_METADATA_PATH)
    return report.model_dump()


@asset
def freshness_result() -> dict:
    """Compute feature freshness from dataset metadata."""
    from monitoring.freshness.compute import compute_freshness

    result = compute_freshness(DATASET_METADATA_PATH)
    return result.model_dump()


@asset
def dataset_lineage() -> dict:
    """Emit a dataset build lineage event."""
    from monitoring.lineage.emitter import (
        build_dataset_lineage_event,
        load_dataset_metadata,
        persist_event,
    )

    meta = load_dataset_metadata(DATASET_METADATA_PATH)
    event = build_dataset_lineage_event(meta)
    log_file = persist_event(event)
    return {"log_file": str(log_file), "event": event.model_dump(mode="json")}


@asset
def training_lineage() -> dict:
    """Emit a training run lineage event."""
    from monitoring.lineage.emitter import (
        build_training_lineage_event,
        load_training_metadata,
        persist_event,
    )

    meta = load_training_metadata(TRAINING_METADATA_PATH)
    event = build_training_lineage_event(meta)
    log_file = persist_event(event)
    return {"log_file": str(log_file), "event": event.model_dump(mode="json")}
