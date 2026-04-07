"""Validation checks for metadata contracts."""

import json
from pathlib import Path

from pydantic import BaseModel


class CheckResult(BaseModel):
    """Single check outcome."""

    check: str
    passed: bool
    detail: str = ""


class CheckReport(BaseModel):
    """Aggregated check results."""

    metadata_path: str
    all_passed: bool
    results: list[CheckResult]


def run_checks(metadata_path: Path) -> CheckReport:
    """Run MVP validation checks against a metadata file.

    Checks:
    1. File exists
    2. Valid JSON
    3. ``dataset_name`` present
    4. ``dataset_version`` present
    5. ``built_at`` present (freshness computable)
    """
    results: list[CheckResult] = []

    # 1. File exists
    if not metadata_path.exists():
        results.append(CheckResult(
            check="file_exists",
            passed=False,
            detail=f"File not found: {metadata_path}",
        ))
        return CheckReport(
            metadata_path=str(metadata_path),
            all_passed=False,
            results=results,
        )

    results.append(CheckResult(check="file_exists", passed=True))

    # 2. Valid JSON
    try:
        data = json.loads(metadata_path.read_text())
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        results.append(CheckResult(
            check="valid_json",
            passed=False,
            detail=str(exc),
        ))
        return CheckReport(
            metadata_path=str(metadata_path),
            all_passed=False,
            results=results,
        )

    results.append(CheckResult(check="valid_json", passed=True))

    # 3. dataset_name present
    has_name = isinstance(data.get("dataset_name"), str) and len(data["dataset_name"]) > 0
    results.append(CheckResult(
        check="dataset_name_present",
        passed=has_name,
        detail="" if has_name else "Missing or empty dataset_name",
    ))

    # 4. dataset_version present
    has_version = isinstance(data.get("dataset_version"), str) and len(data["dataset_version"]) > 0
    results.append(CheckResult(
        check="dataset_version_present",
        passed=has_version,
        detail="" if has_version else "Missing or empty dataset_version",
    ))

    # 5. built_at present (freshness computable)
    has_built_at = isinstance(data.get("built_at"), str) and len(data["built_at"]) > 0
    results.append(CheckResult(
        check="freshness_computable",
        passed=has_built_at,
        detail="" if has_built_at else "Missing built_at — freshness cannot be computed from metadata",
    ))

    return CheckReport(
        metadata_path=str(metadata_path),
        all_passed=all(r.passed for r in results),
        results=results,
    )
