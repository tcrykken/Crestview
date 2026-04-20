# raw_injestion.py


import pandas as pd
from pathlib import Path
from typing import List, Optional, Union
import tempfile
import os
import re
from io import StringIO
import glob

from rental_analytics.utilities.dates import (
    normalize_date_columns_from_config,
    raw_file_creation_date,
)


def preprocess_csv(filename: str, encoding: str = 'utf8') -> str:
    """Remove commas from currency values in CSV"""
    with open(filename, 'r', encoding=encoding) as f:
        content = f.read()
    
    # Remove commas that appear before digits followed by end-of-line or comma
    # This targets currency like $3,575.92
    import re
    content = re.sub(r'(\$\d+),(\d+\.\d+)', r'\1\2', content)
    
    return content

def load_union_ABBexp(
    local_raw_folder: Path,
    file_patterns: Optional[List[str]] = None,
    encoding: str = 'utf8'
) -> pd.DataFrame:
    """
    Load and stack Airbnb transaction/extract CSV files from a local folder.
    
    This function reads CSV files from a local directory, loads them, and stacks
    them together into a single dataframe. Matches the legacy ingestion logic
    that loaded files matching patterns like 'airbnb_t*' and 'airbnb_tax*'.
    
    Args:
        local_raw_folder: Path to local folder containing the CSV files.
        file_patterns: List of filename patterns to match (e.g., ['airbnb_t*', 'airbnb_tax*']).
                      If None, loads all CSV files in the folder.
        encoding: File encoding to use when reading CSVs (default: 'utf8')
    
    Returns:
        Combined dataframe with all extracts stacked together
    
    Examples:
        # Load all CSV files
        df = load_union_ABBexp(
            local_raw_folder=Path('data/raw')
        )
        
        # Load files matching specific patterns
        df = load_union_ABBexp(
            local_raw_folder=Path('data/raw'),
            file_patterns=['airbnb_t*', 'airbnb_tax*']
        )
    """
    import glob
    
    raw_folder = Path(local_raw_folder)
    
    if not raw_folder.exists():
        raise ValueError(f"Local folder does not exist: {raw_folder}")
    
    if not raw_folder.is_dir():
        raise ValueError(f"Path is not a directory: {raw_folder}")
    
    # Find matching files
    if file_patterns:
        all_files = []
        for pattern in file_patterns:
            pattern_path = str(raw_folder / pattern)
            matched_files = glob.glob(pattern_path)
            all_files.extend(matched_files)
        print(f"Found {len(all_files)} files matching patterns: {file_patterns}")
    else:
        # Get all CSV files
        all_files = list(raw_folder.glob("*.csv"))
        all_files = [str(f) for f in all_files]
        print(f"Found {len(all_files)} CSV files in {raw_folder}")
    
    if not all_files:
        raise ValueError(f"No files found in {raw_folder}")
    
    # Load and stack dataframes
    print(f"\nLoading and stacking {len(all_files)} files...")
    dataframes = []
    
    for filename in all_files:
        try:
            print(f"  Loading: {Path(filename).name}")
            df = pd.read_csv(
                filename,
                header=0,
                encoding=encoding
            )
            # Capitalize column names (matching legacy behavior)
            df.columns = df.columns.str.title()
            # Tag with raw source filename and file creation date from metadata
            src_path = Path(filename)
            df["raw_source_file"] = src_path.name
            df["raw_source_file_created_date"] = raw_file_creation_date(src_path)
            # Normalize known Airbnb TXHX date columns before stacking
            df = normalize_date_columns_from_config(
                df,
                config={
                    "cols": [
                        "Payout Date",
                        "Start Date",
                        "End Date",
                        "Confirmation Date",
                    ],
                    "dayfirst": False,
                    "yearfirst": True,
                    "errors": "coerce",
                },
            )
            dataframes.append(df)
            print(f"    Loaded {len(df)} rows, {len(df.columns)} columns")
        except Exception as e:
            print(f"  ⚠ Warning: Failed to load {filename}: {e}")
            continue
    
    if not dataframes:
        raise RuntimeError("No dataframes were successfully loaded")
    
    # Stack all dataframes together
    print(f"\nStacking {len(dataframes)} dataframes...")
    combined_df = pd.concat(
        dataframes,
        axis=0,
        ignore_index=True,
        sort=False
    )
    
    print(f"✓ Combined dataframe: {len(combined_df)} rows, {len(combined_df.columns)} columns")
    
    # Write to staging folder
    staging_folder = raw_folder.parent / "Staging"
    staging_folder.mkdir(parents=True, exist_ok=True)
    
    output_file = staging_folder / "ABB_stackTXHX_00py.csv"
    combined_df.to_csv(output_file, index=False, encoding=encoding)
    print(f"\n✓ Output written to: {output_file}")
    
    return combined_df

def load_union_ABBexp_from_files(
    file_names: List[str],
    local_raw_folder: Path,
    encoding: str = 'utf8'
) -> pd.DataFrame:
    """
    Load and stack Airbnb extracts from specific local files.
    
    Convenience function that loads specific CSV files from a local folder
    by filename.
    
    Args:
        file_names: List of filenames to load (e.g., ['airbnb_transactions.csv', 'airbnb_tax.csv'])
        local_raw_folder: Path to local folder containing the CSV files
        encoding: File encoding to use when reading CSVs (default: 'utf8')
    
    Returns:
        Combined dataframe with all extracts stacked together
    
    Examples:
        # Load specific files
        df = load_union_ABBexp_from_files(
            file_names=['airbnb_transactions.csv', 'airbnb_tax.csv'],
            local_raw_folder=Path('/Users/a2338-home/Documents/Crestview/305Analysis/data/Raw')
        )
    """
    raw_folder = Path(local_raw_folder)
    
    if not raw_folder.exists():
        raise ValueError(f"Local folder does not exist: {raw_folder}")
    
    if not raw_folder.is_dir():
        raise ValueError(f"Path is not a directory: {raw_folder}")
    
    # Validate and build file paths
    files_to_load = []
    for filename in file_names:
        file_path = raw_folder / filename
        
        if not file_path.exists():
            raise ValueError(f"File not found: {file_path}")
        
        files_to_load.append(str(file_path))
    
    if not files_to_load:
        raise ValueError("No files specified to load")
    
    # Load and stack dataframes
    print(f"Loading and stacking {len(files_to_load)} files...")
    dataframes = []
    
    for filename in files_to_load:
        try:
            print(f"  Loading: {Path(filename).name}")
            df = pd.read_csv(
                filename,
                header=0,
                encoding=encoding
            )
            # Capitalize column names (matching legacy behavior)
            df.columns = df.columns.str.title()
            # Tag with raw source filename and file creation date from metadata
            src_path = Path(filename)
            df["raw_source_file"] = src_path.name
            df["raw_source_file_created_date"] = raw_file_creation_date(src_path)
            # Normalize known Airbnb TXHX date columns before stacking
            df = normalize_date_columns_from_config(
                df,
                config={
                    "cols": [
                        "Payout Date",
                        "Start Date",
                        "End Date",
                        "Confirmation Date",
                    ],
                    "dayfirst": False,
                    "yearfirst": True,
                    "errors": "coerce",
                },
            )
            dataframes.append(df)
            print(f"    Loaded {len(df)} rows, {len(df.columns)} columns")
        except Exception as e:
            print(f"  ⚠ Warning: Failed to load {filename}: {e}")
            continue
    
    if not dataframes:
        raise RuntimeError("No dataframes were successfully loaded")
    
    # Stack all dataframes together
    print(f"\nStacking {len(dataframes)} dataframes...")
    combined_df = pd.concat(
        dataframes,
        axis=0,
        ignore_index=True,
        sort=False
    )
    
    print(f"✓ Combined dataframe: {len(combined_df)} rows, {len(combined_df.columns)} columns")
    
    # Write to staging folder
    staging_folder = raw_folder.parent / "Staging"
    staging_folder.mkdir(parents=True, exist_ok=True)
    
    output_file = staging_folder / "ABB_stackTXHX_00py.csv"
    combined_df.to_csv(output_file, index=False, encoding=encoding)
    print(f"\n✓ Output written to: {output_file}")
    
    return combined_df


def load_union_ABBres(
    local_raw_folder: Path,
    file_patterns: Optional[List[str]] = None,
    encoding: str = 'utf8'
) -> pd.DataFrame:
    """
    Load and stack Airbnb reservation CSV files from a local folder.
    
    This function reads CSV files from a local directory, loads them, and stacks
    them together into a single dataframe.
    
    Args:
        local_raw_folder: Path to local folder containing the CSV files.
        file_patterns: List of filename patterns to match (e.g., ['airbnb_res*', 'reservation*']).
                      If None, loads all CSV files in the folder.
        encoding: File encoding to use when reading CSVs (default: 'utf8')
    
    Returns:
        Combined dataframe with all reservation files stacked together
    
    Examples:
        # Load all CSV files
        df = load_union_ABBres(
            local_raw_folder=Path('data/raw')
        )
        
        # Load files matching specific patterns
        df = load_union_ABBres(
            local_raw_folder=Path('data/raw'),
            file_patterns=['airbnb_res*', 'reservation*']
        )
    """
    
    raw_folder = Path(local_raw_folder)
    
    if not raw_folder.exists():
        raise ValueError(f"Local folder does not exist: {raw_folder}")
    
    if not raw_folder.is_dir():
        raise ValueError(f"Path is not a directory: {raw_folder}")

    # # Use the same logic as TXHX but with different default patterns
    # if file_patterns is None:
    #     file_patterns = ['airbnb_res*', 'reservation*', 'res*']
    
    # Find matching files
    if file_patterns:
        all_files = []
        for pattern in file_patterns:
            pattern_path = str(raw_folder / pattern)
            matched_files = glob.glob(pattern_path)
            all_files.extend(matched_files)
        print(f"Found {len(all_files)} files matching patterns: {file_patterns}")
    else:
        # Get all CSV files
        all_files = list(raw_folder.glob("*.csv"))
        all_files = [str(f) for f in all_files]
        print(f"Found {len(all_files)} CSV files in {raw_folder}")
    
    if not all_files:
        raise ValueError(f"No files found in {raw_folder}")
    
    # Load and stack dataframes
    print(f"\nLoading and stacking {len(all_files)} files...")
    dataframes = []
    
    for filename in all_files:
        try:
            print(f"  Loading: {Path(filename).name}")
            preprocessed = preprocess_csv(filename, encoding)
            df = pd.read_csv(
                StringIO(preprocessed),
                header=0,
                encoding=encoding
            )
            # Capitalize column names (matching legacy behavior)
            df.columns = df.columns.str.title()
            # Tag with raw source filename and file creation date from metadata
            src_path = Path(filename)
            df["raw_source_file"] = src_path.name
            df["raw_source_file_created_date"] = raw_file_creation_date(src_path)
            # Normalize known Airbnb reservation date columns before stacking
            df = normalize_date_columns_from_config(
                df,
                config={
                    "cols": [
                        "Start Date",
                        "End Date",
                        "Checkin",
                        "Checkout",
                        "Confirmation Date",
                    ],
                    "dayfirst": False,
                    "yearfirst": True,
                    "errors": "coerce",
                },
            )
            dataframes.append(df)
            print(f"    Loaded {len(df)} rows, {len(df.columns)} columns")
        except Exception as e:
            print(f"  ⚠ Warning: Failed to load {filename}: {e}")
            continue
    
    if not dataframes:
        raise RuntimeError("No dataframes were successfully loaded")
    
    # Stack all dataframes together
    print(f"\nStacking {len(dataframes)} dataframes...")
    combined_df = pd.concat(
        dataframes,
        axis=0,
        ignore_index=True,
        sort=False
    )
    
    print(f"✓ Combined dataframe: {len(combined_df)} rows, {len(combined_df.columns)} columns")
    
    # Write to staging folder
    staging_folder = raw_folder.parent / "Staging"
    staging_folder.mkdir(parents=True, exist_ok=True)
    
    output_file = staging_folder / "ABB_stackres_00py.csv"
    combined_df.to_csv(output_file, index=False, encoding=encoding)
    print(f"\n✓ Output written to: {output_file}")
    
    return combined_df


def load_dailyBalance(
    local_raw_folder: Path,
    file: str = 'bmoDailyBalance.csv',
    encoding: str = 'utf8'
) -> pd.DataFrame:
    """
    Load and stack BMO daily balance CSV file from a local folder.
    
    This function reads a CSV file from a local directory, loads it, and returns a dataframe.
    
    Args:
        local_raw_folder: Path to local folder containing the CSV file.
        file: Name of the CSV file to load (default: 'bmoDailyBalance.csv')
        encoding: File encoding to use when reading CSVs (default: 'utf8')
    """
    raw_folder = Path(local_raw_folder)

    df = pd.read_csv(raw_folder / file, encoding=encoding)

    print(f"{file}:")
    print(df.head())
    df.info()
    for col in ("date", "balance", "ref", "ref_amount"):
        if col in df.columns:
            print(f"{col}:\n{df[col].head()}")
    return df