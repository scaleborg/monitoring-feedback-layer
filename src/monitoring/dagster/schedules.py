"""Dagster schedules for P6 monitoring."""

from dagster import ScheduleDefinition

from monitoring.dagster.jobs import p6_monitoring_job

p6_monitoring_schedule = ScheduleDefinition(
    name="p6_monitoring_schedule",
    job=p6_monitoring_job,
    cron_schedule="17 * * * *",
)
