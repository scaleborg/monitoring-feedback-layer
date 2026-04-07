"""Global configuration constants for the monitoring layer."""

from pathlib import Path

# Default freshness threshold in seconds (30 minutes).
DEFAULT_FRESHNESS_THRESHOLD_SECONDS: int = 1800

# Default environment label applied to all metrics.
DEFAULT_ENVIRONMENT: str = "local"

# Root directory for lineage event logs (relative to repo root).
LINEAGE_LOG_DIR: Path = Path("logs/lineage")
