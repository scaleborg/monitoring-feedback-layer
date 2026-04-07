"""Derive per-deployment serving health state from metrics windows."""

from datetime import datetime, timezone
from itertools import groupby
from operator import attrgetter
from typing import Literal

from pydantic import BaseModel

from monitoring.contracts.serving import ServingMetricsWindow

HealthStatus = Literal[
    "healthy",
    "degraded_latency",
    "degraded_errors",
    "stale",
    "no_traffic",
]

# The field used to partition windows into per-deployment groups.
DEPLOYMENT_GROUP_KEY = "deployment_id"


class ServingHealthThresholds(BaseModel):
    """Configurable thresholds for health classification."""

    stale_window_seconds: float = 300.0
    latency_p95_ms: float = 500.0
    latency_p99_ms: float = 1000.0
    failure_rate_pct: float = 5.0
    rejection_rate_pct: float = 5.0
    expected_window_seconds: float = 60.0
    max_allowed_gap_factor: float = 2.0


class DeploymentHealthState(BaseModel):
    """Health state for a single deployment."""

    deployment_id: str
    model_name: str
    model_version: str
    bundle_id: str
    input_dataset_name: str
    input_dataset_version: str
    status: HealthStatus
    evaluated_windows: int
    latest_window_end: datetime | None
    missing_window_count: int
    detail: str


def _count_missing_windows(
    windows: list[ServingMetricsWindow],
    expected_seconds: float,
    gap_factor: float,
) -> int:
    """Count gaps where consecutive window_starts are spaced > expected * gap_factor."""
    if len(windows) < 2:
        return 0
    max_gap = expected_seconds * gap_factor
    missing = 0
    for i in range(1, len(windows)):
        gap = (windows[i].window_start - windows[i - 1].window_start).total_seconds()
        if gap > max_gap:
            expected_windows = int(gap / expected_seconds) - 1
            missing += max(expected_windows, 1)
    return missing


def classify_deployment(
    windows: list[ServingMetricsWindow],
    thresholds: ServingHealthThresholds,
    eval_time: datetime | None = None,
) -> DeploymentHealthState:
    """Classify health for a single deployment's set of windows.

    All windows must share the same deployment_id. Raises ValueError if
    the list is empty or contains mixed deployment_ids.
    """
    if not windows:
        raise ValueError("Cannot classify empty window list")

    deployment_ids = {w.deployment_id for w in windows}
    if len(deployment_ids) > 1:
        raise ValueError(
            f"classify_deployment received windows from multiple deployments: {sorted(deployment_ids)}"
        )

    eval_time = eval_time or datetime.now(timezone.utc)
    sorted_windows = sorted(windows, key=attrgetter("window_start"))
    latest = sorted_windows[-1]

    # Lineage from latest window
    deployment_id = latest.deployment_id
    model_name = latest.model_name
    model_version = latest.model_version
    bundle_id = latest.bundle_id
    input_dataset_name = latest.input_dataset_name
    input_dataset_version = latest.input_dataset_version

    latest_window_end = latest.window_end
    if latest_window_end.tzinfo is None:
        latest_window_end = latest_window_end.replace(tzinfo=timezone.utc)

    eval_time_aware = eval_time if eval_time.tzinfo else eval_time.replace(tzinfo=timezone.utc)

    missing_count = _count_missing_windows(
        sorted_windows, thresholds.expected_window_seconds, thresholds.max_allowed_gap_factor,
    )

    def _make(status: HealthStatus, detail: str) -> DeploymentHealthState:
        return DeploymentHealthState(
            deployment_id=deployment_id,
            model_name=model_name,
            model_version=model_version,
            bundle_id=bundle_id,
            input_dataset_name=input_dataset_name,
            input_dataset_version=input_dataset_version,
            status=status,
            evaluated_windows=len(sorted_windows),
            latest_window_end=latest_window_end,
            missing_window_count=missing_count,
            detail=detail,
        )

    # Check staleness
    staleness_seconds = (eval_time_aware - latest_window_end).total_seconds()
    if staleness_seconds > thresholds.stale_window_seconds:
        return _make("stale", f"latest window {staleness_seconds:.0f}s old (threshold {thresholds.stale_window_seconds:.0f}s)")

    # Check no_traffic
    total_requests = sum(w.request_count for w in sorted_windows)
    if total_requests == 0:
        return _make("no_traffic", "request_count == 0 across all evaluated windows")

    # Check degraded_latency (on latest window)
    if latest.latency_p95_ms > thresholds.latency_p95_ms:
        return _make("degraded_latency", f"p95={latest.latency_p95_ms:.1f}ms > {thresholds.latency_p95_ms:.1f}ms")
    if latest.latency_p99_ms > thresholds.latency_p99_ms:
        return _make("degraded_latency", f"p99={latest.latency_p99_ms:.1f}ms > {thresholds.latency_p99_ms:.1f}ms")

    # Check degraded_errors (on latest window)
    if latest.request_count > 0:
        failure_rate = (latest.failure_count / latest.request_count) * 100
        rejection_rate = (latest.rejected_count / latest.request_count) * 100
        if failure_rate > thresholds.failure_rate_pct:
            return _make("degraded_errors", f"failure_rate={failure_rate:.1f}% > {thresholds.failure_rate_pct:.1f}%")
        if rejection_rate > thresholds.rejection_rate_pct:
            return _make("degraded_errors", f"rejection_rate={rejection_rate:.1f}% > {thresholds.rejection_rate_pct:.1f}%")

    return _make("healthy", "all checks passed")


def compute_serving_health(
    windows: list[ServingMetricsWindow],
    thresholds: ServingHealthThresholds | None = None,
    eval_time: datetime | None = None,
) -> list[DeploymentHealthState]:
    """Compute per-deployment health states from a list of serving metrics windows.

    Windows are grouped by deployment_id. Each group is classified independently.
    """
    thresholds = thresholds or ServingHealthThresholds()
    sorted_all = sorted(windows, key=attrgetter(DEPLOYMENT_GROUP_KEY))
    results: list[DeploymentHealthState] = []
    for _dep_id, group in groupby(sorted_all, key=attrgetter(DEPLOYMENT_GROUP_KEY)):
        deployment_windows = list(group)
        results.append(classify_deployment(deployment_windows, thresholds, eval_time))
    return results
