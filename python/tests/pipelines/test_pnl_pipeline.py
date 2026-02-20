# test_pnl_pipeline.py

import pytest

from rental_analytics.pipelines import pnl_pipeline
from rental_analytics.data_access import loaders
from rental_analytics.finance import pnl
from rental_analytics.data_access import bank_transactions

import pandas as pd

class DummyLoader:
    """Dummy context manager to patch load_rental_data for testing."""
    def __init__(self, res_df, tx_df, bmo_df, botw_df):
        self.res_df = res_df
        self.tx_df = tx_df
        self.bmo_df = bmo_df
        self.botw_df = botw_df

    def __call__(self, *args, **kwargs):
        return self.res_df, self.tx_df, self.bmo_df, self.botw_df

@pytest.fixture
def example_data():
    res_data = {
        "lst_grp": ["A", "B"],
        "Confirmation code": ["X123", "Y456"],
        "__of_adults": [2, 4],
        "Start_date": ["2023-01-01", "2023-02-01"],
        "End_date": ["2023-01-05", "2023-02-10"],
        "Earnings": [1000.0, 1550.0],
    }
    tx_data = {
        "amount": [800.0, 1150.0],
        "lst_grp": ["A", "B"],
        "Confirmation Code": ["X123", "Y456"],
        "Type": ["payout", "payout"],
        "Date": ["2023-01-06", "2023-02-11"],
    }
    res_df = pd.DataFrame(res_data)
    tx_df = pd.DataFrame(tx_data)
    bmo_df = pd.DataFrame({"POSTED DATE": [], "AMOUNT": [], "DESCRIPTION": []})
    botw_df = pd.DataFrame({})
    return res_df, tx_df, bmo_df, botw_df

def test_run_pnl(monkeypatch, example_data):
    # This test is a lightweight smoke test for the finance entrypoint.
    # Patch the heavy bank processing dependencies so the test stays fast and stable.
    res_df, tx_df, bmo_df, botw_df = example_data
    # Create a dummy combined bank dataframe
    bank_df = pd.DataFrame({"bank_source": ["BMO", "BotW"], "description": ["Test 1", "Test 2"], "amount": [100.0, -50.0]})
    monkeypatch.setattr(pnl, "categorize_transactions_df", lambda df, **kwargs: df.assign(category=pd.Series(dtype=object)))
    monkeypatch.setattr(pnl, "get_category_summary", lambda df, **kwargs: pd.DataFrame())
    pnl.run_pnl(res_df, tx_df, bank_df)

def test_run_pipeline(monkeypatch, example_data):
    res_df, tx_df, bmo_df, botw_df = example_data
    # Mock the new loader functions
    bank_df = pd.DataFrame({"bank_source": ["BMO"], "description": ["Test"], "amount": [100.0]})
    monkeypatch.setattr(loaders, "load_raw_bank_files", lambda: (bmo_df, botw_df))
    monkeypatch.setattr(loaders, "load_staged_data", lambda: (res_df, tx_df, bank_df))
    monkeypatch.setattr(bank_transactions, "process_bank_transactions", lambda **kwargs: (bank_df, {}))
    monkeypatch.setattr(pnl, "run_pnl", lambda *args, **kwargs: None)
    pnl_pipeline.run()

def test_run_main_script(monkeypatch, example_data):
    res_df, tx_df, bmo_df, botw_df = example_data
    monkeypatch.setattr(loaders, "load_rental_data", DummyLoader(res_df, tx_df, bmo_df, botw_df))
    from importlib import reload
    # Simulate __main__ execution: reload the module (won't rerun if already imported as main)
    reload(pnl_pipeline)
