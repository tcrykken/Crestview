from rental_analytics.data_access.transaction_categorization import combine_crosswalks, categorize_transactions_df
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
    # debug
    print(f"BMO df:")
    print(f"BMO df, raw source file creation dates: {bmo_df['raw_source_file_created_date'].unique()}")
    print(f"BotW df, raw source file names: {botw_df['raw_source_file'].unique()}")
    print(f"BotW df, raw source file creation dates: {botw_df['raw_source_file_created_date'].unique()}")
    
    # Step 2: Process and combine bank transactions
    print("\n" + "="*70)
    print("STEP 2: Processing and combining bank transactions")
    print("="*70)
    # debug, delete, check bmo_df and botw_df columns
    print(f"\pnl pipeline - BMO df columns: {bmo_df.columns.tolist()}")
    print(f"\pnl pipeline - BotW df columns: {botw_df.columns.tolist()}")
    combined_bank_df, dq_results = process_bank_transactions(
        bmo_df=bmo_df,
        botw_df=botw_df,
        perform_dq_checks=True,
        verbose=True,
        deduplicate=True,
        raw_folder=project_root / "data" / "Raw",
    )

    # Step 2b: Categorize transactions using crosswalks (prefer finalized_crosswalk.csv if exists)
    print("\n" + "="*70)
    print("STEP 2b: Categorizing transactions using crosswalks (with finalized crosswalk support)")
    print("="*70)
    from rental_analytics.data_access.transaction_categorization import (
        combine_crosswalks, categorize_transactions_df, interactively_categorize_unknowns, export_finalized_crosswalk
    )
    import os
    output_crosswalk_path = project_root / 'data' / 'output' / 'finalized_crosswalk.csv'
    if output_crosswalk_path.exists():
        print(f"Loading crosswalk from {output_crosswalk_path}")
        crosswalks_df = pd.read_csv(output_crosswalk_path)
        # If bank_source missing, add default
        if 'bank_source' not in crosswalks_df.columns:
            crosswalks_df['bank_source'] = 'UNKNOWN'
    else:
        print("No finalized_crosswalk.csv found, loading from Reference crosswalks.")
        crosswalks_df = combine_crosswalks()

    combined_bank_df = categorize_transactions_df(
        combined_bank_df,
        description_col='description',
        bank_source_col='bank_source',
        crosswalks_df=crosswalks_df
    )
    print(f"Sample categorized transactions:\n{combined_bank_df[['description','category']].head()}")

    # Check for unknowns and prompt user to categorize if any
    unknowns = combined_bank_df[combined_bank_df['category'].isna() | (combined_bank_df['category'] == '')]
    if not unknowns.empty:
        print(f"\nFound {len(unknowns)} transactions with unknown categories.\nLaunching interactive categorization tool...")
        # Prompt user to categorize unknowns interactively
        updated_crosswalk = interactively_categorize_unknowns(
            combined_bank_df,
            description_col='description',
            bank_source_col='bank_source',
            category_col='category',
            crosswalks_df=crosswalks_df,
            auto_save=True,
            show_existing_categories=True
        )
        # Save updated crosswalk
        export_finalized_crosswalk(updated_crosswalk, include_bank_source=True)
        # Re-categorize with updated crosswalk
        print("Re-categorizing transactions with updated crosswalk...")
        combined_bank_df = categorize_transactions_df(
            combined_bank_df,
            description_col='description',
            bank_source_col='bank_source',
            crosswalks_df=updated_crosswalk
        )
        print(f"Sample after re-categorization:\n{combined_bank_df[['description','category']].head()}")
    # print(f"Combined bank df, raw source file names: {combined_bank_df['raw_source_file'].unique()}")
    # print(f"Combined bank df, raw source file creation dates: {combined_bank_df['raw_source_file_create_date'].unique()}")

    
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