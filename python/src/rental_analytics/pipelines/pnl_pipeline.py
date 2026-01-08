# pnl_pipeline.py


from rental_analytics.data_access.loaders import load_rental_data
from rental_analytics.data_access.raw_injestion import load_union_ABBexp
from rental_analytics.finance.pnl import run_pnl


def run(use_raw_injestion: bool = False):
    if use_raw_injestion:
        load_union_ABBexp()

    res_df, tx_df, bmo_df, botw_df = load_rental_data()
    # continue with PnL analysis using df

    run_pnl(res_df, tx_df, bmo_df, botw_df)


if __name__ == "__main__":
    run()