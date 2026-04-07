"""Prometheus metrics registry for the monitoring layer."""

from prometheus_client import Counter, Gauge, Histogram

from monitoring.config import DEFAULT_ENVIRONMENT

# --- Histograms ---

ml_dataset_build_duration_seconds = Histogram(
    "ml_dataset_build_duration_seconds",
    "Duration of dataset build operations in seconds",
    labelnames=["dataset_name", "environment"],
)

ml_training_duration_seconds = Histogram(
    "ml_training_duration_seconds",
    "Duration of training runs in seconds",
    labelnames=["model_name", "environment"],
)

# --- Counters ---

ml_prediction_total = Counter(
    "ml_prediction_total",
    "Total number of predictions served",
    labelnames=["model_name", "environment"],
)

# --- Gauges ---

ml_feature_freshness_seconds = Gauge(
    "ml_feature_freshness_seconds",
    "Freshness of the latest dataset build in seconds",
    labelnames=["dataset_name", "environment"],
)


# --- Helper functions ---


def record_dataset_build_duration(
    dataset_name: str,
    duration_seconds: float,
    environment: str = DEFAULT_ENVIRONMENT,
) -> None:
    ml_dataset_build_duration_seconds.labels(
        dataset_name=dataset_name,
        environment=environment,
    ).observe(duration_seconds)


def record_training_duration(
    model_name: str,
    duration_seconds: float,
    environment: str = DEFAULT_ENVIRONMENT,
) -> None:
    ml_training_duration_seconds.labels(
        model_name=model_name,
        environment=environment,
    ).observe(duration_seconds)


def record_prediction(
    model_name: str,
    count: int = 1,
    environment: str = DEFAULT_ENVIRONMENT,
) -> None:
    ml_prediction_total.labels(
        model_name=model_name,
        environment=environment,
    ).inc(count)


def set_feature_freshness(
    dataset_name: str,
    freshness_seconds: float,
    environment: str = DEFAULT_ENVIRONMENT,
) -> None:
    ml_feature_freshness_seconds.labels(
        dataset_name=dataset_name,
        environment=environment,
    ).set(freshness_seconds)
