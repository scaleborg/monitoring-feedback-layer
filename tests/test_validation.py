"""Tests for validation checks."""

import json
from pathlib import Path

from monitoring.validation.checks import run_checks

FIXTURES = Path(__file__).parent / "fixtures"


class TestRunChecks:
    def test_valid_metadata_passes(self):
        report = run_checks(FIXTURES / "dataset_metadata.json")
        assert report.all_passed is True
        assert len(report.results) == 5

    def test_missing_file_fails(self, tmp_path: Path):
        report = run_checks(tmp_path / "nonexistent.json")
        assert report.all_passed is False
        assert report.results[0].check == "file_exists"
        assert report.results[0].passed is False

    def test_invalid_json_fails(self, tmp_path: Path):
        bad = tmp_path / "bad.json"
        bad.write_text("not json{{{")
        report = run_checks(bad)
        assert report.all_passed is False
        assert any(r.check == "valid_json" and not r.passed for r in report.results)

    def test_missing_dataset_name_fails(self, tmp_path: Path):
        data = {"dataset_version": "v1", "built_at": "2026-01-01T00:00:00Z"}
        p = tmp_path / "meta.json"
        p.write_text(json.dumps(data))
        report = run_checks(p)
        assert not report.all_passed
        name_check = next(r for r in report.results if r.check == "dataset_name_present")
        assert not name_check.passed

    def test_missing_built_at_flags_freshness(self, tmp_path: Path):
        data = {"dataset_name": "test", "dataset_version": "v1"}
        p = tmp_path / "meta.json"
        p.write_text(json.dumps(data))
        report = run_checks(p)
        freshness_check = next(r for r in report.results if r.check == "freshness_computable")
        assert not freshness_check.passed
