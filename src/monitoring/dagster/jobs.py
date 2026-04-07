"""Dagster jobs for P6 monitoring."""

from dagster import AssetSelection, define_asset_job

p6_monitoring_job = define_asset_job(
    name="p6_monitoring_job",
    selection=AssetSelection.assets(
        "validation_result",
        "freshness_result",
        "dataset_lineage",
        "training_lineage",
    ),
)
