"""Tests for freshness computation."""

import json
from datetime import datetime, timezone
from pathlib import Path

from monitoring.freshness.compute import compute_freshness

FIXTURES = Path(__file__).parent / "fixtures"


class TestComputeFreshness:
    def test_stale_dataset(self):
        """The fixture has built_at in the past, so it should be STALE."""
        result = compute_freshness(
            FIXTURES / "dataset_metadata.json",
            update_metric=False,
        )
        assert result.dataset_name == "bike_demand_pti"
        assert result.status == "STALE"
        assert result.freshness_seconds > 0

    def test_fresh_with_high_threshold(self):
        """With a very large threshold, any dataset is FRESH."""
        result = compute_freshness(
            FIXTURES / "dataset_metadata.json",
            threshold_seconds=999_999_999,
            update_metric=False,
        )
        assert result.status == "FRESH"

    def test_fresh_dataset(self, tmp_path: Path):
        """Create a metadata file with built_at = now, should be FRESH."""
        now = datetime.now(timezone.utc)
        data = {
            "dataset_name": "test_ds",
            "dataset_version": now.isoformat(),
            "path": "test/data.parquet",
            "built_at": now.isoformat(),
            "row_count": 100,
            "feature_count": 5,
            "target": "t",
            "entity": "e",
            "event_ts": "ts",
            "schema_version": "v1",
        }
        meta_path = tmp_path / "meta.json"
        meta_path.write_text(json.dumps(data))

        result = compute_freshness(meta_path, update_metric=False)
        assert result.status == "FRESH"
        assert result.freshness_seconds < 10
