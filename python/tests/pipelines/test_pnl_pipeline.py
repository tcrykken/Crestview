# test_pnl_pipeline.py

import pytest

from rental_analytics.pipelines import pnl_pipeline
from rental_analytics.data_access import loaders
from rental_analytics.finance import pnl

import pandas as pd

class DummyLoader:
    """Dummy context manager to patch load_rental_data for testing."""
    def __init__(self, res_df, tx_df):
        self.res_df = res_df
        self.tx_df = tx_df

    def __call__(self, *args, **kwargs):
        return self.res_df, self.tx_df

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
    return res_df, tx_df

def test_run_pnl(monkeypatch, example_data):
    res_df, tx_df = example_data
    # Patch data loader that pnl.run_pnl uses
    monkeypatch.setattr(loaders, "load_rental_data", DummyLoader(res_df, tx_df))
    # Capture output: Should not raise, should print reservation and transaction info.
    pnl.run_pnl(res_df, tx_df)

def test_run_pipeline(monkeypatch, example_data):
    res_df, tx_df = example_data
    monkeypatch.setattr(loaders, "load_rental_data", DummyLoader(res_df, tx_df))
    # Also monkeypatch raw_injestion in case it is called
    from rental_analytics.data_access import raw_injestion
    monkeypatch.setattr(raw_injestion, "load_union_ABBexp", lambda: None)

    # Test run function with and without use_raw_injestion
    pnl_pipeline.run(use_raw_injestion=False)
    pnl_pipeline.run(use_raw_injestion=True)

def test_run_main_script(monkeypatch, example_data):
    res_df, tx_df = example_data
    monkeypatch.setattr(loaders, "load_rental_data", DummyLoader(res_df, tx_df))
    from importlib import reload
    # Simulate __main__ execution: reload the module (won't rerun if already imported as main)
    reload(pnl_pipeline)
