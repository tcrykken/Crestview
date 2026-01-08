# pnl.py

from rental_analytics.data_access.loaders import load_rental_data
from rental_analytics.data_access.bank_transactions import process_bank_transactions

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

    # TODO: Add PnL calculation logic using combined_bank_df


if __name__ == "__main__":
    res_df, tx_df, bmo_df, botw_df = load_rental_data()
    run_pnl(res_df, tx_df, bmo_df, botw_df) 