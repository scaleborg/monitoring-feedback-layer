"""Runtime input paths for Dagster assets.

Real artifacts are used where available. Fixtures are reserved for tests.
"""

from pathlib import Path

# --- P2: Real artifact exists ---
# Latest P2 training dataset parquet from mobility-feature-pipeline.
# The adapter (contracts.adapters.adapt_p2_parquet) reads Parquet schema
# metadata and constructs a DatasetMetadata contract.
P2_PARQUET_PATH = Path.home() / "projects/mobility-feature-pipeline/output/training_dataset_20260403_230612.parquet"

# --- P4: No real artifact exists yet ---
# P4 (ml-training-orchestrator) has not produced an artifacts/<run_id>/
# bundle directory. The training_lineage asset uses the expected-format
# fixture until a real training run is executed.
P4_TRAINING_METADATA_PATH = Path("tests/fixtures/training_metadata.json")
