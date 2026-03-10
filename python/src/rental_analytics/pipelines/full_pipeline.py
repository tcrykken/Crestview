"""End-to-end pipeline: ingest raw Airbnb data, stage it, process bank data,
and run the P&L analysis.

This module:
1. Loads and stacks raw Airbnb reservation CSVs into `data/Staging/ABB_stackres_00py.csv`
2. Loads and stacks raw Airbnb TXHX / payout CSVs into `data/Staging/ABB_stackTXHX_00py.csv`
3. Runs the existing bank + P&L pipeline (`pnl_pipeline.run`)
"""

from pathlib import Path

from rental_analytics.data_access.raw_injestion import (
    load_union_ABBres,
    load_union_ABBexp,
)
from rental_analytics.pipelines.pnl_pipeline import run as run_pnl_pipeline


def run() -> None:
    """Run the full end-to-end data pipeline."""
    # Project root: 5 levels up from this file (pipelines -> rental_analytics -> src -> python -> 305Analysis)
    project_root = Path(__file__).parent.parent.parent.parent.parent
    raw_folder = project_root / "data" / "Raw"

    print("=" * 70)
    print("STEP 0: Ingesting raw Airbnb reservation data")
    print("=" * 70)
    load_union_ABBres(local_raw_folder=raw_folder,file_patterns=['airbnb_res*', '*reservation*', '*rez*', '_ABnB_Res*', '_ABnb_Res*'])

    print("\n" + "=" * 70)
    print("STEP 0b: Ingesting raw Airbnb TXHX / payout data")
    print("=" * 70)
    load_union_ABBexp(local_raw_folder=raw_folder,file_patterns=['airbnb_t*', 'airbnb_tax*'])

    print("\n" + "=" * 70)
    print("STEP 1+: Running bank processing and P&L pipeline")
    print("=" * 70)
    run_pnl_pipeline()


if __name__ == "__main__":
    run()

