"""Serving metrics window contract — P5 → P6.

Mirrors the P5 ServingMetricsWindow schema defined in
mobility-serving-layer/app/schemas/observability.py and the contract in
mobility-serving-layer/contracts/serving_observability.md.
"""

from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, Field, model_validator


class ServingMetricsWindow(BaseModel):
    """Schema for a single serving metrics window emitted by P5.

    All required/optional fields and constraints match the P5 contract
    (serving_observability.md schema_version v1).
    """

    schema_version: Literal["v1"]
    window_start: datetime
    window_end: datetime
    service_name: str
    service_version: str
    environment: str
    deployment_id: str
    model_name: str
    model_version: str
    bundle_id: str
    input_dataset_name: str
    input_dataset_version: str

    # Request metrics
    endpoint_name: str
    request_count: int = Field(ge=0)
    success_count: int = Field(ge=0)
    failure_count: int = Field(ge=0)
    rejected_count: int = Field(ge=0)
    timeout_count: int = Field(ge=0)

    # Latency metrics
    latency_p50_ms: float = Field(ge=0.0)
    latency_p95_ms: float = Field(ge=0.0)
    latency_p99_ms: float = Field(ge=0.0)
    feature_lookup_p95_ms: float | None = Field(default=None, ge=0.0)
    model_exec_p95_ms: float | None = Field(default=None, ge=0.0)

    # Error breakdown
    validation_error_count: int = Field(ge=0)
    feature_lookup_error_count: int = Field(ge=0)
    model_load_error_count: int = Field(ge=0)
    inference_runtime_error_count: int = Field(ge=0)
    dependency_error_count: int = Field(ge=0)
    internal_error_count: int = Field(ge=0)

    # Input quality
    input_schema_failure_count: int = Field(ge=0)
    missing_required_field_count: int = Field(ge=0)
    invalid_type_count: int = Field(ge=0)
    domain_violation_count: int = Field(ge=0)

    # Prediction quality
    prediction_count: int = Field(ge=0)
    prediction_null_count: int = Field(ge=0)
    prediction_non_finite_count: int = Field(ge=0)
    prediction_out_of_range_count: int = Field(ge=0)
    fallback_prediction_count: int = Field(ge=0)

    # Feature quality (optional)
    missing_feature_count: int | None = Field(default=None, ge=0)
    default_imputed_feature_count: int | None = Field(default=None, ge=0)
    stale_feature_count: int | None = Field(default=None, ge=0)
    feature_vector_build_failure_count: int | None = Field(default=None, ge=0)

    # Heartbeat
    heartbeat_emitted_at: datetime

    @model_validator(mode="after")
    def _validate_window_order(self) -> "ServingMetricsWindow":
        if self.window_end <= self.window_start:
            raise ValueError(
                f"window_end ({self.window_end}) must be after window_start ({self.window_start})"
            )
        return self

    @model_validator(mode="after")
    def _validate_timestamps_aware(self) -> "ServingMetricsWindow":
        for name in ("window_start", "window_end", "heartbeat_emitted_at"):
            ts = getattr(self, name)
            if ts.tzinfo is None:
                raise ValueError(f"{name} must be timezone-aware (got naive datetime)")
            setattr(self, name, ts.astimezone(timezone.utc))
        return self

    @model_validator(mode="after")
    def _validate_non_empty_strings(self) -> "ServingMetricsWindow":
        for name in (
            "service_name",
            "service_version",
            "environment",
            "deployment_id",
            "model_name",
            "model_version",
            "bundle_id",
            "input_dataset_name",
            "input_dataset_version",
            "endpoint_name",
        ):
            val = getattr(self, name)
            if not val or not val.strip():
                raise ValueError(f"{name} must not be empty")
        return self
