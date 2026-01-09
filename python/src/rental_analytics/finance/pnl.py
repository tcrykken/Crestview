# pnl.py

import pandas as pd
from rental_analytics.data_access.loaders import load_rental_data
from rental_analytics.data_access.bank_transactions import process_bank_transactions
from rental_analytics.data_access.transaction_categorization import (
    categorize_transactions_df,
    get_category_summary,
)

def run_pnl(res_df, tx_df, bmo_df, botw_df) -> None:
    """
    Entry point for the PnL analysis given reservation and transaction data.

    Args:
        res_df (pd.DataFrame): DataFrame containing reservation data.
        tx_df (pd.DataFrame): DataFrame containing transaction data.
        bmo_df (pd.DataFrame): DataFrame containing bmo bank transaction data.
        botw_df (pd.DataFrame): DataFrame containing botw bank transaction data.
    """
    # Process and join bank transactions
    print("Processing bank transactions...")
    combined_bank_df, dq_results = process_bank_transactions(
        bmo_df=bmo_df,
        botw_df=botw_df,
        perform_dq_checks=True,
        verbose=True
    )

    # Placeholder for PnL analysis logic
    print("\n" + "="*60)
    print("RESERVATION DATA")
    print("="*60)
    print("res_df:")
    print(res_df.head(3))
    print(f"Shape: {res_df.shape}")

    print("\n" + "="*60)
    print("TRANSACTION DATA")
    print("="*60)
    print("tx_df:")
    print(tx_df.head(3))
    print(f"Shape: {tx_df.shape}")

    print("\n" + "="*60)
    print("COMBINED BANK TRANSACTIONS")
    print("="*60)
    print("combined_bank_df:")
    print(combined_bank_df.head(10))
    print(f"Shape: {combined_bank_df.shape}")
    print(f"\nBank source distribution:")
    print(combined_bank_df['bank_source'].value_counts())

    # Categorize transactions
    print("\n" + "="*60)
    print("TRANSACTION CATEGORIZATION")
    print("="*60)
    print("Categorizing transactions using crosswalks...")
    categorized_df = categorize_transactions_df(
        combined_bank_df,
        description_col='description',
        bank_source_col='bank_source',
        category_col='category'
    )

    # Show categorization coverage
    categorized_count = categorized_df['category'].notna().sum()
    total_count = len(categorized_df)
    coverage_pct = (categorized_count / total_count * 100) if total_count > 0 else 0

    print(f"\nCategorization Results:")
    print(f"  Total transactions: {total_count:,}")
    print(f"  Categorized: {categorized_count:,} ({coverage_pct:.1f}%)")
    print(f"  Uncategorized: {total_count - categorized_count:,} ({100 - coverage_pct:.1f}%)")

    # Show category distribution
    print("\nCategory Distribution:")
    category_counts = categorized_df['category'].value_counts()
    for category, count in category_counts.head(15).items():
        if pd.notna(category):
            pct = (count / total_count * 100)
            print(f"  {category}: {count:,} ({pct:.1f}%)")

    # Generate and display category summary
    print("\n" + "="*60)
    print("CATEGORY SUMMARY (for PnL Reporting)")
    print("="*60)
    summary = get_category_summary(categorized_df)
    print(summary.to_string(index=False))

    # TODO: Add PnL calculation logic using categorized_df


if __name__ == "__main__":
    res_df, tx_df, bmo_df, botw_df = load_rental_data()
    run_pnl(res_df, tx_df, bmo_df, botw_df) 