"""Runtime input paths for Dagster assets.

Real artifacts are used where available. Fixtures are reserved for tests.
"""

from pathlib import Path

# --- P2: Real artifact exists ---
# Latest P2 training dataset parquet from mobility-feature-pipeline.
# The adapter (contracts.adapters.adapt_p2_parquet) reads Parquet schema
# metadata and constructs a DatasetMetadata contract.
P2_PARQUET_PATH = Path.home() / "projects/mobility-feature-pipeline/output/training_dataset_20260403_230612.parquet"

# --- P4: Real artifact exists ---
# First real P4 inference bundle produced by train-and-register pipeline.
# The adapter (contracts.adapters.adapt_p4_bundle) reads metadata.json +
# metrics.json from the bundle directory and constructs a TrainingMetadata contract.
P4_BUNDLE_PATH = Path.home() / "projects/ml-training-orchestrator/artifacts/85ec755803a448fea3d2efb49b66ce5e"

# --- P5: Serving metrics artifact tree ---
# P5 repo root. The ingestion module reads from the date/hour/minute
# artifact layout under {P5_BASE_DIR}/artifacts/serving/metrics/.
# See: mobility-serving-layer/contracts/serving_observability.md
P5_BASE_DIR = Path.home() / "Projects/mobility-serving-layer"
