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
P4_BUNDLE_PATH = Path.home() / "projects/ml-training-orchestrator/artifacts/eea15be8c4b74bf5a6229a114e330c97"
