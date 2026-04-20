# loaders.py

import pandas as pd
from pathlib import Path

from rental_analytics.utilities.dates import (
    normalize_date_columns_from_config,
    raw_file_creation_date,
)


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

    Discovers all BMO and BotW CSV files by pattern (BMO*.csv, BotW*.csv, BOTW*.csv),
    loads and stacks them (with raw_source_file set per file), then returns one
    BMO dataframe and one BotW dataframe for downstream processing.

    These are the source files that get processed and combined into
    data/Staging/combined_bank_transactions.csv by the pipeline.

    Returns:
        bmo_df (pd.DataFrame): DataFrame containing all BMO bank transaction data.
        botw_df (pd.DataFrame): DataFrame containing all BotW bank transaction data.
    """
    project_root = Path(__file__).parent.parent.parent.parent.parent
    raw_dir = project_root / "data" / "Raw"

    if not raw_dir.exists() or not raw_dir.is_dir():
        raise FileNotFoundError(f"Raw data directory does not exist: {raw_dir}")

    all_raw_csvs = list(raw_dir.glob("*.csv"))

    # Discover BMO files (case-insensitive: BMO, bmo, Bmo_..., etc.)
    bmo_paths = sorted(p for p in all_raw_csvs if p.name.upper().startswith("BMO_"))
    if not bmo_paths:
        raise FileNotFoundError(
            f"No BMO CSV files found in {raw_dir}. Expecting files matching BMO_*.csv"
        )

    bmo_dfs = []
    for bmo_file_path in bmo_paths:
        try:
            df = pd.read_csv(bmo_file_path, encoding="UTF-8", sep=",")
        except Exception as e:
            raise RuntimeError(f"Failed to load CSV from {bmo_file_path}: {e}") from e
        df["raw_source_file"] = bmo_file_path.name
        df["raw_source_file_created_date"] = raw_file_creation_date(bmo_file_path)
        df = normalize_date_columns_from_config(
            df,
            config={
                "cols": [
                    "Date",
                    "Posting Date",
                    "POSTED DATE",
                ],
                "dayfirst": False,
                "yearfirst": True,
                "errors": "coerce",
            },
        )
        bmo_dfs.append(df)

    bmo_df = pd.concat(bmo_dfs, axis=0, ignore_index=True, sort=False)

    # Discover BotW files (case-insensitive: BotW, BOTW, botw, BOTW_tx_..., BOTW_txhx_..., etc.)
    botw_paths = sorted(
        p for p in all_raw_csvs if p.name.upper().startswith("BOTW")
    )
    if not botw_paths:
        raise FileNotFoundError(
            f"No BotW CSV files found in {raw_dir}. Expecting files matching BotW*.csv or BOTW*.csv"
        )

    botw_dfs = []
    for botw_file_path in botw_paths:
        try:
            df = pd.read_csv(botw_file_path)
        except Exception as e:
            raise RuntimeError(f"Failed to load CSV from {botw_file_path}: {e}") from e
        df["raw_source_file"] = botw_file_path.name
        df["raw_source_file_created_date"] = raw_file_creation_date(botw_file_path)
        df = normalize_date_columns_from_config(
            df,
            config={
                "cols": [
                    "Post Date",
                    "Trans Date",
                    "Date",
                ],
                "dayfirst": False,
                "yearfirst": True,
                "errors": "coerce",
            },
        )
        botw_dfs.append(df)

    botw_df = pd.concat(botw_dfs, axis=0, ignore_index=True, sort=False)

    # debug, remove, check columns in bmo_df and botw_df
    print(f"\n load_raw_bank_files - BMO df columns: {bmo_df.columns.tolist()}")
    print(f"load_raw_bank_files - BotW df columns: {botw_df.columns.tolist()}")

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