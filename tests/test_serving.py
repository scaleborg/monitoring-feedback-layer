"""Tests for P5 serving metrics ingestion and health derivation."""

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from monitoring.contracts.serving import ServingMetricsWindow
from monitoring.serving.health import (
    DEPLOYMENT_GROUP_KEY,
    DeploymentHealthState,
    ServingHealthThresholds,
    classify_deployment,
    compute_serving_health,
    _compute_max_gap_minutes,
    _count_missing_windows,
)
from monitoring.serving.ingest import ingest_serving_metrics


def _make_window(**overrides) -> dict:
    """Build a valid serving metrics window dict matching the real P5 schema."""
    base = {
        "schema_version": "v1",
        "window_start": "2026-04-07T10:00:00Z",
        "window_end": "2026-04-07T10:01:00Z",
        "service_name": "mobility-serving-layer",
        "service_version": "0.1.0",
        "environment": "production",
        "deployment_id": "dep-001",
        "endpoint_name": "/predict",
        "model_name": "bike_demand_model",
        "model_version": "v1",
        "bundle_id": "bundle-abc",
        "input_dataset_name": "bike_demand_pti",
        "input_dataset_version": "2026-04-07T08:00:00Z",
        "request_count": 100,
        "success_count": 98,
        "failure_count": 1,
        "rejected_count": 1,
        "timeout_count": 0,
        "latency_p50_ms": 12.0,
        "latency_p95_ms": 45.0,
        "latency_p99_ms": 120.0,
        "validation_error_count": 0,
        "feature_lookup_error_count": 0,
        "model_load_error_count": 0,
        "inference_runtime_error_count": 0,
        "dependency_error_count": 0,
        "internal_error_count": 0,
        "input_schema_failure_count": 0,
        "missing_required_field_count": 0,
        "invalid_type_count": 0,
        "domain_violation_count": 0,
        "prediction_count": 98,
        "prediction_null_count": 0,
        "prediction_non_finite_count": 0,
        "prediction_out_of_range_count": 0,
        "fallback_prediction_count": 0,
        "heartbeat_emitted_at": "2026-04-07T10:01:00Z",
    }
    base.update(overrides)
    return base


def _build_p5_tree(tmp_path: Path, records: list[dict]) -> Path:
    """Build a P5-layout artifact tree under tmp_path and return the base_dir.

    Layout: {base_dir}/artifacts/serving/metrics/{date}/{hour}/metrics_{minute}.jsonl
    Each record is placed according to its window_start timestamp.
    """
    base_dir = tmp_path / "p5"
    for record in records:
        ws = datetime.fromisoformat(record["window_start"])
        date_str = ws.strftime("%Y-%m-%d")
        hour_str = ws.strftime("%H")
        minute_str = ws.strftime("%M")
        metrics_dir = base_dir / "artifacts" / "serving" / "metrics" / date_str / hour_str
        metrics_dir.mkdir(parents=True, exist_ok=True)
        jsonl_path = metrics_dir / f"metrics_{minute_str}.jsonl"
        with jsonl_path.open("a") as f:
            f.write(json.dumps(record) + "\n")
    return base_dir


# ── A. Ingestion ──────────────────────────────────────────────────────


class TestServingMetricsIngestion:
    def test_valid_ingestion(self, tmp_path):
        """Records from P5 artifact tree load and validate successfully."""
        w1 = _make_window(
            window_start="2026-04-07T10:00:00Z",
            window_end="2026-04-07T10:01:00Z",
        )
        w2 = _make_window(
            window_start="2026-04-07T10:01:00Z",
            window_end="2026-04-07T10:02:00Z",
        )
        base = _build_p5_tree(tmp_path, [w1, w2])
        records = ingest_serving_metrics(base)
        assert len(records) == 2
        assert all(isinstance(r, ServingMetricsWindow) for r in records)

    def test_preserves_utc_timestamps(self, tmp_path):
        base = _build_p5_tree(tmp_path, [_make_window()])
        records = ingest_serving_metrics(base)
        for r in records:
            assert r.window_start.tzinfo is not None
            assert r.window_end.tzinfo is not None

    def test_preserves_lineage_fields(self, tmp_path):
        base = _build_p5_tree(tmp_path, [_make_window()])
        records = ingest_serving_metrics(base)
        r = records[0]
        assert r.deployment_id == "dep-001"
        assert r.model_version == "v1"
        assert r.bundle_id == "bundle-abc"
        assert r.input_dataset_name == "bike_demand_pti"
        assert r.input_dataset_version == "2026-04-07T08:00:00Z"

    def test_preserves_p5_specific_fields(self, tmp_path):
        """P5-specific fields (schema_version, service_name, etc.) are retained."""
        base = _build_p5_tree(tmp_path, [_make_window()])
        records = ingest_serving_metrics(base)
        r = records[0]
        assert r.schema_version == "v1"
        assert r.service_name == "mobility-serving-layer"
        assert r.environment == "production"
        assert r.input_schema_failure_count == 0

    def test_schema_rejection_missing_field(self, tmp_path):
        """Record missing a required field raises ValueError."""
        bad = _make_window()
        del bad["deployment_id"]
        base = _build_p5_tree(tmp_path, [bad])
        with pytest.raises(ValueError, match="schema validation failed"):
            ingest_serving_metrics(base)

    def test_schema_rejection_invalid_json(self, tmp_path):
        """Malformed JSON line raises ValueError."""
        base = tmp_path / "p5"
        metrics_dir = base / "artifacts" / "serving" / "metrics" / "2026-04-07" / "10"
        metrics_dir.mkdir(parents=True)
        (metrics_dir / "metrics_00.jsonl").write_text("{not valid json}\n")
        with pytest.raises(ValueError, match="invalid JSON"):
            ingest_serving_metrics(base)

    def test_dedup_on_stable_key(self, tmp_path):
        """Duplicate records with same (deployment_id, endpoint_name, window_start, window_end) are deduplicated."""
        w = _make_window()
        # Write same record 3 times into the same JSONL file
        base = tmp_path / "p5"
        metrics_dir = base / "artifacts" / "serving" / "metrics" / "2026-04-07" / "10"
        metrics_dir.mkdir(parents=True)
        jsonl = metrics_dir / "metrics_00.jsonl"
        jsonl.write_text((json.dumps(w) + "\n") * 3)
        records = ingest_serving_metrics(base)
        assert len(records) == 1

    def test_dedup_preserves_different_endpoints(self, tmp_path):
        """Same deployment+window but different endpoint_name are kept separate."""
        w1 = _make_window(endpoint_name="/predict")
        w2 = _make_window(endpoint_name="/predict-batch")
        base = _build_p5_tree(tmp_path, [w1, w2])
        records = ingest_serving_metrics(base)
        assert len(records) == 2

    def test_directory_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            ingest_serving_metrics(tmp_path / "nonexistent")

    def test_empty_metrics_dir(self, tmp_path):
        """Metrics directory exists but has no JSONL files → empty list."""
        metrics_dir = tmp_path / "artifacts" / "serving" / "metrics"
        metrics_dir.mkdir(parents=True)
        records = ingest_serving_metrics(tmp_path)
        assert records == []

    def test_reads_across_multiple_date_dirs(self, tmp_path):
        """Records spread across different date/hour directories are all collected."""
        w1 = _make_window(
            window_start="2026-04-07T10:00:00Z",
            window_end="2026-04-07T10:01:00Z",
        )
        w2 = _make_window(
            window_start="2026-04-07T11:00:00Z",
            window_end="2026-04-07T11:01:00Z",
        )
        w3 = _make_window(
            window_start="2026-04-08T09:00:00Z",
            window_end="2026-04-08T09:01:00Z",
        )
        base = _build_p5_tree(tmp_path, [w1, w2, w3])
        records = ingest_serving_metrics(base)
        assert len(records) == 3

    def test_rejects_bad_schema_version(self, tmp_path):
        """schema_version != 'v1' is rejected."""
        bad = _make_window(schema_version="v2")
        base = _build_p5_tree(tmp_path, [bad])
        with pytest.raises(ValueError, match="schema validation failed"):
            ingest_serving_metrics(base)

    def test_rejects_window_end_not_after_start(self, tmp_path):
        """window_end <= window_start is rejected."""
        bad = _make_window(
            window_start="2026-04-07T10:01:00Z",
            window_end="2026-04-07T10:01:00Z",
        )
        base = _build_p5_tree(tmp_path, [bad])
        with pytest.raises(ValueError, match="schema validation failed"):
            ingest_serving_metrics(base)

    def test_rejects_empty_string_field(self, tmp_path):
        """Empty required string field is rejected."""
        bad = _make_window(deployment_id="")
        base = _build_p5_tree(tmp_path, [bad])
        with pytest.raises(ValueError, match="schema validation failed"):
            ingest_serving_metrics(base)

    def test_rejects_whitespace_only_string_field(self, tmp_path):
        """Whitespace-only required string field is rejected."""
        bad = _make_window(model_name="   ")
        base = _build_p5_tree(tmp_path, [bad])
        with pytest.raises(ValueError, match="schema validation failed"):
            ingest_serving_metrics(base)

    def test_rejects_naive_timestamp(self):
        """Naive (no timezone) timestamps are rejected at the contract level."""
        raw = _make_window(window_start="2026-04-07T10:00:00")
        with pytest.raises(Exception):
            ServingMetricsWindow.model_validate(raw)


# ── B. Freshness / completeness ──────────────────────────────────────


class TestFreshnessAndGaps:
    def test_stale_detection(self):
        """Window older than staleness threshold → unhealthy."""
        w = ServingMetricsWindow.model_validate(_make_window(
            window_start="2026-04-07T08:59:00Z",
            window_end="2026-04-07T09:00:00Z",
        ))
        thresholds = ServingHealthThresholds(max_staleness_minutes=5.0)
        # Eval 10 minutes after window_end → unhealthy
        eval_time = datetime(2026, 4, 7, 9, 10, 0, tzinfo=timezone.utc)
        state = classify_deployment([w], thresholds, eval_time)
        assert state.status == "unhealthy"
        assert "staleness" in state.detail

    def test_not_stale_within_threshold(self):
        """Window within staleness threshold → not unhealthy."""
        w = ServingMetricsWindow.model_validate(_make_window(
            window_start="2026-04-07T09:59:00Z",
            window_end="2026-04-07T10:00:00Z",
        ))
        thresholds = ServingHealthThresholds(max_staleness_minutes=5.0)
        eval_time = datetime(2026, 4, 7, 10, 2, 0, tzinfo=timezone.utc)
        state = classify_deployment([w], thresholds, eval_time)
        assert state.status != "unhealthy"

    def test_missing_window_gap_detection(self):
        """3-minute gap in 60s cadence → detected missing windows."""
        w1 = ServingMetricsWindow.model_validate(_make_window(
            window_start="2026-04-07T10:00:00Z",
            window_end="2026-04-07T10:01:00Z",
        ))
        # 3-minute gap: skip 10:01 and 10:02 windows
        w2 = ServingMetricsWindow.model_validate(_make_window(
            window_start="2026-04-07T10:03:00Z",
            window_end="2026-04-07T10:04:00Z",
        ))
        count = _count_missing_windows(
            [w1, w2],
            expected_seconds=60.0,
            gap_factor=2.0,
        )
        assert count == 2

    def test_no_gaps_in_continuous_windows(self):
        """Consecutive 60s windows → 0 missing."""
        windows = [
            ServingMetricsWindow.model_validate(_make_window(
                window_start=f"2026-04-07T10:0{i}:00Z",
                window_end=f"2026-04-07T10:0{i+1}:00Z",
            ))
            for i in range(3)
        ]
        count = _count_missing_windows(windows, expected_seconds=60.0, gap_factor=2.0)
        assert count == 0


# ── C. Health classification ──────────────────────────────────────────


class TestHealthClassification:
    def test_degraded_latency_p95(self):
        """p95 above threshold → degraded."""
        w = ServingMetricsWindow.model_validate(_make_window(
            latency_p95_ms=600.0,
            window_end="2026-04-07T10:01:00Z",
        ))
        thresholds = ServingHealthThresholds(latency_p95_ms=500.0)
        eval_time = datetime(2026, 4, 7, 10, 2, 0, tzinfo=timezone.utc)
        state = classify_deployment([w], thresholds, eval_time)
        assert state.status == "degraded"
        assert "p95" in state.detail

    def test_degraded_latency_p99(self):
        """p99 above threshold → degraded."""
        w = ServingMetricsWindow.model_validate(_make_window(
            latency_p95_ms=400.0,  # p95 OK
            latency_p99_ms=1200.0,  # p99 bad
            window_end="2026-04-07T10:01:00Z",
        ))
        thresholds = ServingHealthThresholds(latency_p99_ms=1000.0)
        eval_time = datetime(2026, 4, 7, 10, 2, 0, tzinfo=timezone.utc)
        state = classify_deployment([w], thresholds, eval_time)
        assert state.status == "degraded"
        assert "p99" in state.detail

    def test_degraded_errors_failure_rate(self):
        """Failure rate above threshold → degraded."""
        w = ServingMetricsWindow.model_validate(_make_window(
            request_count=100,
            failure_count=10,
            rejected_count=0,
            window_end="2026-04-07T10:01:00Z",
        ))
        thresholds = ServingHealthThresholds(failure_rate_pct=5.0)
        eval_time = datetime(2026, 4, 7, 10, 2, 0, tzinfo=timezone.utc)
        state = classify_deployment([w], thresholds, eval_time)
        assert state.status == "degraded"
        assert "failure_rate" in state.detail

    def test_degraded_errors_rejection_rate(self):
        """Rejection rate above threshold → degraded."""
        w = ServingMetricsWindow.model_validate(_make_window(
            request_count=100,
            failure_count=0,
            rejected_count=8,
            window_end="2026-04-07T10:01:00Z",
        ))
        thresholds = ServingHealthThresholds(rejection_rate_pct=5.0)
        eval_time = datetime(2026, 4, 7, 10, 2, 0, tzinfo=timezone.utc)
        state = classify_deployment([w], thresholds, eval_time)
        assert state.status == "degraded"
        assert "rejection_rate" in state.detail

    def test_no_traffic(self):
        """request_count == 0 → unhealthy."""
        w = ServingMetricsWindow.model_validate(_make_window(
            request_count=0,
            success_count=0,
            failure_count=0,
            rejected_count=0,
            window_end="2026-04-07T10:01:00Z",
        ))
        thresholds = ServingHealthThresholds()
        eval_time = datetime(2026, 4, 7, 10, 2, 0, tzinfo=timezone.utc)
        state = classify_deployment([w], thresholds, eval_time)
        assert state.status == "unhealthy"

    def test_healthy(self):
        """All metrics within thresholds → healthy."""
        w = ServingMetricsWindow.model_validate(_make_window(
            window_end="2026-04-07T10:01:00Z",
        ))
        thresholds = ServingHealthThresholds()
        eval_time = datetime(2026, 4, 7, 10, 2, 0, tzinfo=timezone.utc)
        state = classify_deployment([w], thresholds, eval_time)
        assert state.status == "healthy"
        assert state.detail == "all checks passed"

    def test_healthy_preserves_lineage(self):
        """Health state preserves deployment lineage fields."""
        w = ServingMetricsWindow.model_validate(_make_window(
            window_end="2026-04-07T10:01:00Z",
        ))
        eval_time = datetime(2026, 4, 7, 10, 2, 0, tzinfo=timezone.utc)
        state = classify_deployment([w], ServingHealthThresholds(), eval_time)
        assert state.deployment_id == "dep-001"
        assert state.model_version == "v1"
        assert state.bundle_id == "bundle-abc"
        assert state.input_dataset_name == "bike_demand_pti"
        assert state.input_dataset_version == "2026-04-07T08:00:00Z"


# ── C2. Hardened health: gaps, missing windows, staleness ────────────


class TestHardenedHealth:
    """Tests for gap-aware, missing-window-aware, staleness-aware classification."""

    def test_healthy_continuous_windows(self):
        """Continuous windows with good metrics → healthy."""
        windows = [
            ServingMetricsWindow.model_validate(_make_window(
                window_start=f"2026-04-07T10:0{i}:00Z",
                window_end=f"2026-04-07T10:0{i+1}:00Z",
            ))
            for i in range(5)
        ]
        thresholds = ServingHealthThresholds(
            max_missing_windows=3,
            max_gap_minutes=5.0,
            max_staleness_minutes=10.0,
        )
        eval_time = datetime(2026, 4, 7, 10, 6, 0, tzinfo=timezone.utc)
        state = classify_deployment(windows, thresholds, eval_time)
        assert state.status == "healthy"
        assert state.detail == "all checks passed"
        assert state.missing_window_count == 0

    def test_degraded_due_to_gap(self):
        """Large gap between consecutive windows → degraded."""
        w1 = ServingMetricsWindow.model_validate(_make_window(
            window_start="2026-04-07T10:00:00Z",
            window_end="2026-04-07T10:01:00Z",
        ))
        # 8-minute gap
        w2 = ServingMetricsWindow.model_validate(_make_window(
            window_start="2026-04-07T10:08:00Z",
            window_end="2026-04-07T10:09:00Z",
        ))
        thresholds = ServingHealthThresholds(
            max_missing_windows=100,  # high so gap check triggers first
            max_gap_minutes=5.0,
            max_staleness_minutes=10.0,
        )
        eval_time = datetime(2026, 4, 7, 10, 10, 0, tzinfo=timezone.utc)
        state = classify_deployment([w1, w2], thresholds, eval_time)
        assert state.status == "degraded"
        assert "max_gap" in state.detail

    def test_degraded_due_to_missing_windows(self):
        """Too many missing windows → degraded."""
        w1 = ServingMetricsWindow.model_validate(_make_window(
            window_start="2026-04-07T10:00:00Z",
            window_end="2026-04-07T10:01:00Z",
        ))
        # 6-minute gap → ~5 missing windows in 60s cadence
        w2 = ServingMetricsWindow.model_validate(_make_window(
            window_start="2026-04-07T10:06:00Z",
            window_end="2026-04-07T10:07:00Z",
        ))
        thresholds = ServingHealthThresholds(
            max_missing_windows=2,
            max_gap_minutes=10.0,  # high gap threshold so gap check doesn't trigger first
            max_staleness_minutes=10.0,
        )
        eval_time = datetime(2026, 4, 7, 10, 8, 0, tzinfo=timezone.utc)
        state = classify_deployment([w1, w2], thresholds, eval_time)
        assert state.status == "degraded"
        assert "missing_windows" in state.detail

    def test_unhealthy_due_to_long_inactivity(self):
        """Staleness exceeding max_staleness_minutes → unhealthy."""
        w = ServingMetricsWindow.model_validate(_make_window(
            window_start="2026-04-07T09:00:00Z",
            window_end="2026-04-07T09:01:00Z",
        ))
        thresholds = ServingHealthThresholds(max_staleness_minutes=10.0)
        # 30 minutes after last window
        eval_time = datetime(2026, 4, 7, 9, 31, 0, tzinfo=timezone.utc)
        state = classify_deployment([w], thresholds, eval_time)
        assert state.status == "unhealthy"
        assert "staleness" in state.detail

    def test_unhealthy_takes_priority_over_degraded(self):
        """When both staleness and gaps are bad, unhealthy wins."""
        w1 = ServingMetricsWindow.model_validate(_make_window(
            window_start="2026-04-07T08:00:00Z",
            window_end="2026-04-07T08:01:00Z",
        ))
        w2 = ServingMetricsWindow.model_validate(_make_window(
            window_start="2026-04-07T08:10:00Z",
            window_end="2026-04-07T08:11:00Z",
        ))
        thresholds = ServingHealthThresholds(
            max_missing_windows=2,
            max_gap_minutes=5.0,
            max_staleness_minutes=10.0,
        )
        # 50 minutes after last window → unhealthy
        eval_time = datetime(2026, 4, 7, 9, 0, 0, tzinfo=timezone.utc)
        state = classify_deployment([w1, w2], thresholds, eval_time)
        assert state.status == "unhealthy"

    def test_max_gap_minutes_helper(self):
        """_compute_max_gap_minutes returns correct max gap."""
        windows = [
            ServingMetricsWindow.model_validate(_make_window(
                window_start="2026-04-07T10:00:00Z",
                window_end="2026-04-07T10:01:00Z",
            )),
            ServingMetricsWindow.model_validate(_make_window(
                window_start="2026-04-07T10:01:00Z",
                window_end="2026-04-07T10:02:00Z",
            )),
            ServingMetricsWindow.model_validate(_make_window(
                window_start="2026-04-07T10:05:00Z",
                window_end="2026-04-07T10:06:00Z",
            )),
        ]
        assert _compute_max_gap_minutes(windows) == 4.0

    def test_max_gap_minutes_single_window(self):
        """Single window → 0 gap."""
        w = ServingMetricsWindow.model_validate(_make_window())
        assert _compute_max_gap_minutes([w]) == 0.0

    def test_degraded_missing_windows_boundary(self):
        """Exactly at max_missing_windows → not degraded (must exceed)."""
        w1 = ServingMetricsWindow.model_validate(_make_window(
            window_start="2026-04-07T10:00:00Z",
            window_end="2026-04-07T10:01:00Z",
        ))
        # 4-minute gap → 3 missing windows in 60s cadence
        w2 = ServingMetricsWindow.model_validate(_make_window(
            window_start="2026-04-07T10:04:00Z",
            window_end="2026-04-07T10:05:00Z",
        ))
        thresholds = ServingHealthThresholds(
            max_missing_windows=3,
            max_gap_minutes=10.0,
            max_staleness_minutes=10.0,
        )
        eval_time = datetime(2026, 4, 7, 10, 6, 0, tzinfo=timezone.utc)
        state = classify_deployment([w1, w2], thresholds, eval_time)
        assert state.status == "healthy"


# ── D. Multi-deployment ──────────────────────────────────────────────


class TestMultiDeploymentHealth:
    def test_group_key_is_deployment_id(self):
        """The grouping constant is deployment_id."""
        assert DEPLOYMENT_GROUP_KEY == "deployment_id"

    def test_classify_rejects_mixed_deployments(self):
        """classify_deployment raises if windows span multiple deployment_ids."""
        w1 = ServingMetricsWindow.model_validate(_make_window(
            deployment_id="dep-001",
            window_end="2026-04-07T10:01:00Z",
        ))
        w2 = ServingMetricsWindow.model_validate(_make_window(
            deployment_id="dep-002",
            window_end="2026-04-07T10:01:00Z",
        ))
        eval_time = datetime(2026, 4, 7, 10, 2, 0, tzinfo=timezone.utc)
        with pytest.raises(ValueError, match="multiple deployments"):
            classify_deployment([w1, w2], ServingHealthThresholds(), eval_time)

    def test_per_deployment_granularity(self):
        """Two deployments produce two separate health states."""
        w1 = ServingMetricsWindow.model_validate(_make_window(
            deployment_id="dep-001",
            window_end="2026-04-07T10:01:00Z",
        ))
        w2 = ServingMetricsWindow.model_validate(_make_window(
            deployment_id="dep-002",
            request_count=0,
            success_count=0,
            failure_count=0,
            rejected_count=0,
            window_end="2026-04-07T10:01:00Z",
        ))
        eval_time = datetime(2026, 4, 7, 10, 2, 0, tzinfo=timezone.utc)
        states = compute_serving_health([w1, w2], eval_time=eval_time)
        assert len(states) == 2
        by_dep = {s.deployment_id: s for s in states}
        assert by_dep["dep-001"].status == "healthy"
        assert by_dep["dep-002"].status == "unhealthy"
