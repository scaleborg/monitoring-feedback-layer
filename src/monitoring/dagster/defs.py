"""Dagster definitions entrypoint for P6 monitoring."""

from dagster import Definitions

from monitoring.dagster.assets import (
    dataset_lineage,
    freshness_result,
    training_lineage,
    validation_result,
)
from monitoring.dagster.jobs import p6_monitoring_job
from monitoring.dagster.schedules import p6_monitoring_schedule
from monitoring.dagster.serving_assets import serving_health_state, serving_metrics_raw

defs = Definitions(
    assets=[
        validation_result,
        freshness_result,
        dataset_lineage,
        training_lineage,
        serving_metrics_raw,
        serving_health_state,
    ],
    jobs=[p6_monitoring_job],
    schedules=[p6_monitoring_schedule],
)
