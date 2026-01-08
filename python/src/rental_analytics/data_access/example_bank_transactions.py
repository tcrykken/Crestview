"""Example script demonstrating bank transaction processing.

This script shows how to use the bank_transactions module to join
BMO and BotW data and perform data quality checks.
"""

from rental_analytics.data_access.loaders import load_rental_data
from rental_analytics.data_access.bank_transactions import process_bank_transactions


def main():
    """Example usage of bank transaction processing."""
    # Load all data
    res_df, tx_df, bmo_df, botw_df = load_rental_data()

    # Process bank transactions (join + DQ checks)
    combined_bank_df, dq_results = process_bank_transactions(
        bmo_df=bmo_df,
        botw_df=botw_df,
        perform_dq_checks=True,
        verbose=True
    )

    print(f"\nCombined bank transactions dataframe shape: {combined_bank_df.shape}")
    print(f"\nFirst few records:")
    print(combined_bank_df.head())

    return combined_bank_df, dq_results


if __name__ == "__main__":
    main()

