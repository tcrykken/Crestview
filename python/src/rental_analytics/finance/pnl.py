# pnl.py

from rental_analytics.data_access.loaders import load_rental_data

def run_pnl(res_df, tx_df, bmo_df, botw_df) -> None:
    """
    Entry point for the PnL analysis given reservation and transaction data.

    Args:
        res_df (pd.DataFrame): DataFrame containing reservation data.
        tx_df (pd.DataFrame): DataFrame containing transaction data.
        bmo_df (pd.DataFrame): DataFrame containing bmo bank transaction data.
        botw_df (pd.DataFrame): DataFrame containing botw bank transaction data.
    """
    # Placeholder for PnL analysis logic
    print("res_df:")
    print(res_df.head(3))
    print(res_df['Confirmation code'])
    print(res_df)

    print("\ntx_df:")
    print(tx_df.head(3))
    print(tx_df.columns.values)
    print(tx_df['Confirmation Code'])
    print(tx_df)

    print("\nbmo_df:")
    print(bmo_df.head(3))
    print(bmo_df.columns.values)
    print(bmo_df['DESCRIPTION'])
    print(bmo_df)

    print("\nbotw_df:")
    print(botw_df.head(3))


if __name__ == "__main__":
    res_df, tx_df, bmo_df, botw_df = load_rental_data()
    run_pnl(res_df, tx_df, bmo_df, botw_df) 