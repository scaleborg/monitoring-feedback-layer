"""Tests for Dagster definitions loading."""

from monitoring.dagster.defs import defs


def test_definitions_load():
    assert defs is not None


def test_job_exists():
    assert defs.get_job_def("p6_monitoring_job") is not None


def test_schedule_exists():
    assert defs.get_schedule_def("p6_monitoring_schedule") is not None
