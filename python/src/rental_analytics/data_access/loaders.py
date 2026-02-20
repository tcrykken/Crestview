# loaders.py

import pandas as pd
from pathlib import Path


def load_staged_data():
    """
    Load all staged (pre-processed) data files from data/Staging/.
    
    Returns:
        res_df (pd.DataFrame): DataFrame containing reservation data.
        tx_df (pd.DataFrame): DataFrame containing transaction data.
        bank_df (pd.DataFrame): DataFrame containing combined bank transaction data.
    """
    # Get project root (5 levels up from this file: data_access -> rental_analytics -> src -> python -> 305Analysis)
    project_root = Path(__file__).parent.parent.parent.parent.parent

    # Staged (pre-stacked) Airbnb reservations
    res_file_path = project_root / 'data' / 'Staging' / 'ABB_stackres_00py.csv'

    try:
        res_df = pd.read_csv(res_file_path)
    except FileNotFoundError:
        raise FileNotFoundError(f"Could not find file at path: {res_file_path}")
    except Exception as e:
        raise RuntimeError(f"Failed to load CSV from {res_file_path}: {e}")

    # Staged (pre-stacked) Airbnb transaction history (TXHX)
    tx_file_path = project_root / 'data' / 'Staging' / 'ABB_stackTXHX_00py.csv'

    try:
        tx_df = pd.read_csv(tx_file_path, encoding='mac_roman', sep='\t')
    except FileNotFoundError:
        raise FileNotFoundError(f"Could not find file at path: {tx_file_path}") 
    except Exception as e:
        raise RuntimeError(f"Failed to load CSV from {tx_file_path}: {e}")

    # Staged (combined and processed) bank transactions
    bank_file_path = project_root / 'data' / 'Staging' / 'combined_bank_transactions.csv'

    try:
        bank_df = pd.read_csv(bank_file_path)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Could not find combined bank transactions file at path: {bank_file_path}\n"
            f"Run the pipeline first to generate this file from raw bank data."
        )
    except Exception as e:
        raise RuntimeError(f"Failed to load CSV from {bank_file_path}: {e}")

    return res_df, tx_df, bank_df


def load_raw_bank_files():
    """
    Load raw bank transaction files from data/Raw/.
    
    These are the source files that get processed and combined into
    data/Staging/combined_bank_transactions.csv by the pipeline.
    
    Returns:
        bmo_df (pd.DataFrame): DataFrame containing BMO bank transaction data.
        botw_df (pd.DataFrame): DataFrame containing BotW bank transaction data.
    """
    # Get project root
    project_root = Path(__file__).parent.parent.parent.parent.parent

    # Raw BMO bank transaction file
    bmo_file_path = project_root / 'data' / 'Raw' / 'BMO_transactions_202309_202512.csv'

    try:
        bmo_df = pd.read_csv(bmo_file_path, encoding='UTF-8', sep=',')
    except FileNotFoundError:
        raise FileNotFoundError(f"Could not find file at path: {bmo_file_path}")
    except Exception as e:
        raise RuntimeError(f"Failed to load CSV from {bmo_file_path}: {e}")

    # Raw BotW bank transaction file
    botw_file_path = project_root / 'data' / 'Raw' / 'BotW_FSF_TX_full_join_27JAN24.csv'

    try:
        botw_df = pd.read_csv(botw_file_path)
    except FileNotFoundError:
        raise FileNotFoundError(f"Could not find file at path: {botw_file_path}")
    except Exception as e:
        raise RuntimeError(f"Failed to load CSV from {botw_file_path}: {e}")

    return bmo_df, botw_df


def load_rental_data():
    """
    Legacy function for backward compatibility.
    
    Loads staged data and returns in the old format (with separate bmo_df and botw_df).
    Note: bank_df is duplicated as both bmo_df and botw_df for compatibility.
    
    Returns:
        res_df (pd.DataFrame): DataFrame containing reservation data.
        tx_df (pd.DataFrame): DataFrame containing transaction data.
        bmo_df (pd.DataFrame): DataFrame containing combined bank transaction data (same as botw_df).
        botw_df (pd.DataFrame): DataFrame containing combined bank transaction data (same as bmo_df).
    """
    res_df, tx_df, bank_df = load_staged_data()
    # Return bank_df as both bmo_df and botw_df for backward compatibility
    return res_df, tx_df, bank_df, bank_df