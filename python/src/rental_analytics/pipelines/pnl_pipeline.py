"""P&L pipeline entrypoint.

This pipeline:
1. Loads raw bank files from `data/Raw/` (BMO and BotW)
2. Processes and combines them into a single stacked file
3. Saves the combined bank transactions to `data/Staging/`
4. Loads all staged files (ABB reservations, ABB TXHX, combined bank transactions)
5. Runs P&L analysis
"""

from pathlib import Path
import pandas as pd

from rental_analytics.data_access.loaders import load_raw_bank_files, load_staged_data
from rental_analytics.data_access.bank_transactions import process_bank_transactions
from rental_analytics.finance.pnl import run_pnl


def run() -> None:
    """
    Run the P&L analysis pipeline.
    
    This function:
    1. Loads raw bank files from data/Raw/
    2. Processes and combines bank transactions, saving to data/Staging/
    3. Loads all staged data files
    4. Runs P&L analysis
    """
    # Get project root
    project_root = Path(__file__).parent.parent.parent.parent.parent
    staging_dir = project_root / 'data' / 'Staging'
    staging_dir.mkdir(parents=True, exist_ok=True)
    
    # Step 1: Load raw bank files
    print("="*70)
    print("STEP 1: Loading raw bank transaction files")
    print("="*70)
    bmo_df, botw_df = load_raw_bank_files()
    
    # Step 2: Process and combine bank transactions
    print("\n" + "="*70)
    print("STEP 2: Processing and combining bank transactions")
    print("="*70)
    combined_bank_df, dq_results = process_bank_transactions(
        bmo_df=bmo_df,
        botw_df=botw_df,
        perform_dq_checks=True,
        verbose=True,
        deduplicate=True
    )
    
    # Step 3: Save combined bank transactions to Staging
    bank_staging_file = staging_dir / 'combined_bank_transactions.csv'
    combined_bank_df.to_csv(bank_staging_file, index=False)
    print(f"\n✓ Saved combined bank transactions to: {bank_staging_file}")
    print(f"  Total records: {len(combined_bank_df):,}")
    
    # Step 4: Load all staged data files
    print("\n" + "="*70)
    print("STEP 3: Loading staged data files")
    print("="*70)
    res_df, tx_df, bank_df = load_staged_data()
    
    # Step 5: Run P&L analysis (run_pnl expects combined bank data)
    run_pnl(res_df, tx_df, bank_df)


if __name__ == "__main__":
    run()