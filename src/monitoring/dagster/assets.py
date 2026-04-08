"""Dagster assets wrapping existing P6 monitoring logic."""

from dagster import asset

from monitoring.dagster.config import P2_PARQUET_PATH, P4_BUNDLE_PATH


@asset
def validation_result() -> dict:
    """Run validation checks against real P2 dataset metadata."""
    from monitoring.contracts.adapters import adapt_p2_parquet
    from monitoring.validation.checks import run_checks

    # Adapt real P2 parquet → DatasetMetadata, then write a temp JSON for
    # the validation checker which expects a metadata JSON path.
    meta = adapt_p2_parquet(P2_PARQUET_PATH)

    # run_checks operates on a JSON file; serialize the adapted contract
    # to a temporary file so existing logic is reused without modification.
    import json
    import tempfile
    from pathlib import Path

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        f.write(meta.model_dump_json())
        tmp_path = Path(f.name)

    report = run_checks(tmp_path)
    tmp_path.unlink()
    return report.model_dump()


@asset
def freshness_result() -> dict:
    """Compute feature freshness from real P2 dataset metadata."""
    from monitoring.contracts.adapters import adapt_p2_parquet
    from monitoring.freshness.compute import compute_freshness

    # Adapt real P2 parquet → DatasetMetadata, then serialize to temp JSON
    # for the freshness compute function.
    import tempfile
    from pathlib import Path

    meta = adapt_p2_parquet(P2_PARQUET_PATH)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        f.write(meta.model_dump_json())
        tmp_path = Path(f.name)

    result = compute_freshness(tmp_path)
    tmp_path.unlink()
    return result.model_dump()


@asset
def dataset_lineage() -> dict:
    """Emit a dataset build lineage event from real P2 artifact."""
    from monitoring.contracts.adapters import adapt_p2_parquet
    from monitoring.lineage.emitter import build_dataset_lineage_event, persist_event

    meta = adapt_p2_parquet(P2_PARQUET_PATH)
    event = build_dataset_lineage_event(meta)
    log_file = persist_event(event)
    return {"log_file": str(log_file), "event": event.model_dump(mode="json")}


@asset
def training_lineage() -> dict:
    """Emit a training run lineage event from real P4 artifact."""
    from monitoring.contracts.adapters import adapt_p4_bundle
    from monitoring.lineage.emitter import build_training_lineage_event, persist_event

    meta = adapt_p4_bundle(P4_BUNDLE_PATH)
    event = build_training_lineage_event(meta)
    log_file = persist_event(event)
    return {"log_file": str(log_file), "event": event.model_dump(mode="json")}
