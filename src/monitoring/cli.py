"""CLI entrypoint for the monitoring layer."""

from pathlib import Path

import typer

app = typer.Typer(
    name="monitoring",
    help="P6 — Monitoring and feedback layer for the urban mobility ML platform.",
    no_args_is_help=True,
)


@app.command()
def emit_lineage_dataset_build(
    metadata_path: Path = typer.Option(..., help="Path to dataset build metadata JSON"),
) -> None:
    """Emit a lineage event for a dataset build."""
    from monitoring.lineage.emitter import (
        build_dataset_lineage_event,
        load_dataset_metadata,
        persist_event,
    )
    meta = load_dataset_metadata(metadata_path)
    event = build_dataset_lineage_event(meta)
    log_file = persist_event(event)
    typer.echo(f"Lineage event written to {log_file}")
    typer.echo(event.model_dump_json(indent=2))


@app.command()
def emit_lineage_training_run(
    metadata_path: Path = typer.Option(..., help="Path to training run metadata JSON"),
) -> None:
    """Emit a lineage event for a training run."""
    from monitoring.lineage.emitter import (
        build_training_lineage_event,
        load_training_metadata,
        persist_event,
    )
    from monitoring.metrics.registry import record_training_duration

    meta = load_training_metadata(metadata_path)
    event = build_training_lineage_event(meta)
    log_file = persist_event(event)

    duration = (meta.completed_at - meta.started_at).total_seconds()
    record_training_duration(meta.model_name, duration)

    typer.echo(f"Lineage event written to {log_file}")
    typer.echo(event.model_dump_json(indent=2))


@app.command()
def compute_freshness(
    metadata_path: Path = typer.Option(..., help="Path to dataset metadata JSON"),
    threshold: int = typer.Option(1800, help="Freshness threshold in seconds"),
) -> None:
    """Compute feature freshness from dataset metadata."""
    from monitoring.freshness.compute import compute_freshness as _compute

    result = _compute(metadata_path, threshold_seconds=threshold)
    typer.echo(result.model_dump_json(indent=2))


@app.command()
def run_checks(
    metadata_path: Path = typer.Option(..., help="Path to metadata JSON to validate"),
) -> None:
    """Run validation checks against a metadata file."""
    from monitoring.validation.checks import run_checks as _run_checks

    report = _run_checks(metadata_path)
    typer.echo(report.model_dump_json(indent=2))
    if not report.all_passed:
        raise typer.Exit(code=1)


@app.command()
def serve_metrics(
    port: int = typer.Option(8000, help="Port to serve metrics on"),
) -> None:
    """Start a Prometheus-compatible metrics HTTP server."""
    from monitoring.metrics.server import serve_metrics as _serve

    _serve(port=port)


@app.command()
def demo_metrics(
    port: int = typer.Option(8000, help="Port to serve metrics on"),
) -> None:
    """Populate sample metrics and start the Prometheus HTTP server."""
    from monitoring.metrics.server import serve_metrics as _serve

    populate_demo_metrics()
    typer.echo("Populated sample metrics for demo_dataset / demo_model")
    _serve(port=port)


def populate_demo_metrics() -> None:
    """Record one sample value for each registered metric."""
    from monitoring.metrics.registry import (
        record_dataset_build_duration,
        record_prediction,
        record_training_duration,
        set_feature_freshness,
    )

    record_dataset_build_duration("demo_dataset", 120.0)
    record_training_duration("demo_model", 150.0)
    record_prediction("demo_model", count=10)
    set_feature_freshness("demo_dataset", 300.0)


@app.command()
def simulate_prediction(
    count: int = typer.Option(1, help="Number of predictions to simulate"),
    model_name: str = typer.Option("bike_demand_model", help="Model name label"),
) -> None:
    """Simulate prediction events and increment the prediction counter."""
    from monitoring.metrics.registry import record_prediction

    record_prediction(model_name, count=count)
    typer.echo(f"Recorded {count} prediction(s) for model={model_name}")
