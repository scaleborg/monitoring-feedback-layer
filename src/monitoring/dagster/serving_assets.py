"""Dagster assets for P5 serving metrics ingestion and health derivation."""

from dagster import asset

from monitoring.dagster.config import P5_BASE_DIR


@asset
def serving_metrics_raw() -> list[dict]:
    """Ingest and validate P5 serving_metrics_window JSONL artifacts.

    Reads from the P5 artifact tree at:
        {P5_BASE_DIR}/artifacts/serving/metrics/{YYYY-MM-DD}/{HH}/metrics_{MM}.jsonl
    """
    from monitoring.serving.ingest import ingest_serving_metrics

    records = ingest_serving_metrics(P5_BASE_DIR)
    return [r.model_dump(mode="json") for r in records]


@asset(deps=[serving_metrics_raw])
def serving_health_state(serving_metrics_raw: list[dict]) -> list[dict]:
    """Derive per-deployment health state from ingested serving metrics."""
    from monitoring.contracts.serving import ServingMetricsWindow
    from monitoring.serving.health import compute_serving_health

    windows = [ServingMetricsWindow.model_validate(r) for r in serving_metrics_raw]
    states = compute_serving_health(windows)
    return [s.model_dump(mode="json") for s in states]
