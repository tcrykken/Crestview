# loaders.py

import pandas as pd
from pathlib import Path

def load_rental_data() -> pd.DataFrame:
    """
    Load rental data from a local CSV file.

    Returns:
        res_df (pd.DataFrame): DataFrame containing reservation data.
        tx_df (pd.DataFrame): DataFrame containing transaction data.
        bmo_df (pd.DataFrame): DataFrame containing bmo bank transaction data.
        botw_df (pd.DataFrame): DataFrame containing botw bank transaction data.
    """
    # Get project root (5 levels up from this file: data_access -> rental_analytics -> src -> python -> 305Analysis)
    project_root = Path(__file__).parent.parent.parent.parent.parent

    res_file_path = project_root / 'data' / 'Raw' / 'ABB_stackres_00py.csv'

    try:
        res_df = pd.read_csv(res_file_path)
    except FileNotFoundError:
        raise FileNotFoundError(f"Could not find file at path: {res_file_path}")
    except Exception as e:
        raise RuntimeError(f"Failed to load CSV from {res_file_path}: {e}")

    tx_file_path = project_root / 'data' / 'Raw' / 'ABB_stackTXHX_00py.csv'

    try:
        tx_df = pd.read_csv(tx_file_path, encoding='mac_roman', sep='\t')
    except FileNotFoundError:
        raise FileNotFoundError(f"Could not find file at path: {tx_file_path}") 
    except Exception as e:
        raise RuntimeError(f"Failed to load CSV from {tx_file_path}: {e}")

    bmo_file_path = project_root / 'data' / 'Raw' / 'BMO_transactions_202309_202512.csv'

    try:
        bmo_df = pd.read_csv(bmo_file_path, encoding='UTF-8', sep=',')
    except FileNotFoundError:
        raise FileNotFoundError(f"Could not find file at path: {bmo_file_path}")
    except Exception as e:
        raise RuntimeError(f"Failed to load CSV from {bmo_file_path}: {e}")

    botw_file_path = project_root / 'data' / 'Raw' / 'BotW_FSF_TX_full_join_27JAN24.csv'

    try:
        botw_df = pd.read_csv(botw_file_path)
    except FileNotFoundError:
        raise FileNotFoundError(f"Could not find file at path: {botw_file_path}")
    except Exception as e:
        raise RuntimeError(f"Failed to load CSV from {botw_file_path}: {e}")

    return res_df, tx_df, bmo_df, botw_df