"""Ingest P5 serving_metrics_window JSONL artifacts.

P5 writes metrics windows to a date/hour/minute directory layout:
    {base_dir}/artifacts/serving/metrics/{YYYY-MM-DD}/{HH}/metrics_{MM}.jsonl

This module globs across that tree, validates each record against the
ServingMetricsWindow contract, deduplicates on a stable key, and returns
the sorted result.
"""

import json
from pathlib import Path

from pydantic import ValidationError

from monitoring.contracts.serving import ServingMetricsWindow

# Relative path from P5 repo root to the metrics artifact tree.
_METRICS_REL = Path("artifacts") / "serving" / "metrics"


def _dedup_key(record: ServingMetricsWindow) -> tuple[str, str, str, str]:
    """Stable dedup key: (deployment_id, endpoint_name, window_start, window_end)."""
    return (
        record.deployment_id,
        record.endpoint_name,
        record.window_start.isoformat(),
        record.window_end.isoformat(),
    )


def _read_jsonl(path: Path) -> list[dict]:
    """Read all non-empty lines from a JSONL file, returning parsed dicts."""
    results: list[dict] = []
    for line_num, line in enumerate(path.read_text().splitlines(), start=1):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            results.append(json.loads(stripped))
        except json.JSONDecodeError as exc:
            raise ValueError(f"{path}:{line_num}: invalid JSON — {exc}") from exc
    return results


def ingest_serving_metrics(p5_base_dir: Path) -> list[ServingMetricsWindow]:
    """Read, validate, and deduplicate serving metrics from the P5 artifact tree.

    Args:
        p5_base_dir: Root of the P5 repository (e.g. ~/projects/mobility-serving-layer).
                     Metrics are read from {p5_base_dir}/artifacts/serving/metrics/**/*.jsonl.

    Raises:
        FileNotFoundError: If the metrics directory does not exist.
        ValueError: On malformed JSON or schema validation failure.

    Returns:
        Deduplicated list sorted by (deployment_id, window_start).
    """
    metrics_dir = p5_base_dir / _METRICS_REL
    if not metrics_dir.exists():
        raise FileNotFoundError(f"P5 metrics directory not found: {metrics_dir}")

    jsonl_files = sorted(metrics_dir.glob("**/*.jsonl"))
    if not jsonl_files:
        return []

    records: list[ServingMetricsWindow] = []
    seen: set[tuple[str, str, str, str]] = set()

    for jsonl_path in jsonl_files:
        for line_num, raw in enumerate(_read_jsonl(jsonl_path), start=1):
            try:
                record = ServingMetricsWindow.model_validate(raw)
            except ValidationError as exc:
                raise ValueError(
                    f"{jsonl_path}:{line_num}: schema validation failed — {exc}"
                ) from exc

            key = _dedup_key(record)
            if key not in seen:
                seen.add(key)
                records.append(record)

    records.sort(key=lambda r: (r.deployment_id, r.window_start))
    return records
