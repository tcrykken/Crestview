"""Example script demonstrating transaction categorization.

This script shows how to use the transaction categorization module
to categorize bank transactions using crosswalk reference data.
"""

import pandas as pd
from rental_analytics.data_access.loaders import load_rental_data
from rental_analytics.data_access.bank_transactions import process_bank_transactions
from rental_analytics.data_access.transaction_categorization import (
    combine_crosswalks,
    categorize_transactions_df,
    get_category_summary,
)


def main():
    """Example usage of transaction categorization."""
    # Load all data
    res_df, tx_df, bmo_df, botw_df = load_rental_data()

    # Process bank transactions (join + DQ checks + deduplication)
    combined_bank_df, dq_results = process_bank_transactions(
        bmo_df=bmo_df,
        botw_df=botw_df,
        perform_dq_checks=True,
        verbose=True,
        deduplicate=True
    )

    print("\n" + "=" * 60)
    print("TRANSACTION CATEGORIZATION")
    print("=" * 60)

    # Load and combine crosswalks
    print("\nLoading crosswalks...")
    crosswalks_df = combine_crosswalks()
    print(f"Loaded {len(crosswalks_df)} categorization patterns")
    print(f"Unique categories: {crosswalks_df['category'].nunique()}")

    # Categorize transactions
    print("\nCategorizing transactions...")
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
    for category, count in category_counts.head(10).items():
        if pd.notna(category):
            pct = (count / total_count * 100)
            print(f"  {category}: {count:,} ({pct:.1f}%)")

    # Generate summary by category
    print("\n" + "=" * 60)
    print("CATEGORY SUMMARY")
    print("=" * 60)
    summary = get_category_summary(categorized_df)
    print(summary.to_string(index=False))

    return categorized_df, summary


if __name__ == "__main__":
    main()

