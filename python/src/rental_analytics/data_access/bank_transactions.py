"""Bank transaction data processing and data quality checks.

This module handles joining BMO and BotW bank transaction dataframes,
standardizing their formats, and performing data quality checks.
"""

from __future__ import annotations

import random
import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


def deduplicate_transactions_with_tracing(df: pd.DataFrame, trace=True):
    """
    Deduplicate transactions, keeping the oldest record by raw_source_file_created_date, with tracing.
    Args:
        df: DataFrame of transactions
        trace: If True, print trace logs
    Returns:
        Deduplicated DataFrame
    """
    df = df.copy()
    # Prepare deduplication keys
    df['amount_rounded'] = pd.to_numeric(df['amount'], errors='coerce').round()
    df['description_substr'] = df['description'].fillna('').astype(str).str[:30]
    df['raw_source_file_created_date'] = pd.to_datetime(df['raw_source_file_created_date'], errors='coerce')

    key_columns = ['transaction_date', 'amount_rounded', 'description_substr']

    # Sort so oldest file comes first
    df = df.sort_values(by=key_columns + ['raw_source_file_created_date'], ascending=True)

    # Find duplicates before dropping
    duplicate_mask = df.duplicated(subset=key_columns, keep=False)
    duplicates = df[duplicate_mask]

    if trace:
        print(f"[TRACE] Found {duplicates.shape[0]} duplicate rows (by {key_columns})")
        if not duplicates.empty:
            print("[TRACE] Example duplicate group:")
            # debug, delete comment below if it is not printing full dup rows to terminal - looks like this does print all records ... idk
            # print(duplicates.groupby(key_columns).head(2).to_string(index=False))

    # Drop duplicates, keeping the oldest (first after sort)
    deduped = df.drop_duplicates(subset=key_columns, keep='first')

    if trace:
        print(f"[TRACE] Deduplicated: {df.shape[0] - deduped.shape[0]} rows removed. Final shape: {deduped.shape}")

    # Optionally, drop helper columns before returning
    return deduped.drop(columns=['amount_rounded', 'description_substr'])


def _parse_to_normalized_calendar_dates(series: pd.Series) -> pd.Series:
    """
    Parse heterogeneous date values to timezone-naive datetimes at midnight (calendar day).

    Used so ``bmoDailyBalance`` ``date`` values align with ``transaction_date`` from
    BMO/BotW exports (e.g. ISO ``2026-01-05``, ``M/D/YYYY`` with or without zero-padding,
    and numeric Excel serial days from spreadsheet exports).

    Merge keys compare these normalized timestamps; only the calendar date matters.
    """
    s = series
    if pd.api.types.is_numeric_dtype(s):
        num = pd.to_numeric(s, errors="coerce")
        from_excel = pd.to_datetime(num, unit="D", origin="1899-12-30", errors="coerce")
        from_mixed = pd.to_datetime(s, errors="coerce", format="mixed")
        out = from_excel.where(from_excel.notna(), from_mixed)
    else:
        out = pd.to_datetime(s, errors="coerce", format="mixed")

    # Timezone-aware values: normalize in UTC then drop tz for stable merge keys
    if getattr(out.dtype, "tz", None) is not None:
        out = out.dt.tz_convert("UTC").dt.normalize().dt.tz_localize(None)
        return out

    return out.dt.normalize()


def standardize_bmo_df(bmo_df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize BMO transaction dataframe format.

    Args:
        bmo_df: Raw BMO transaction dataframe

    Returns:
        Standardized BMO dataframe with consistent column names and types
    """
    df = bmo_df.copy()

    # Standardize column names (handle spaces)
    df.columns = df.columns.str.strip()

    # Convert POSTED DATE to datetime (mixed formats: 01/05/2026, 1/5/2026, ISO, etc.)
    if 'POSTED DATE' in df.columns:
        df['transaction_date'] = _parse_to_normalized_calendar_dates(df['POSTED DATE'])
    else:
        raise ValueError("BMO dataframe missing 'POSTED DATE' column")

    # Standardize amount - ensure numeric
    if 'AMOUNT' in df.columns:
        df['amount'] = pd.to_numeric(df['AMOUNT'], errors='coerce')
    else:
        raise ValueError("BMO dataframe missing 'AMOUNT' column")

    # Create standardized columns
    df['bank_source'] = 'BMO'
    df['description'] = df.get('DESCRIPTION', '')
    df['currency'] = df.get('CURRENCY', 'USD')
    df['transaction_reference'] = df.get('TRANSACTION REFERENCE NUMBER', '')

    df['transaction_type'] = df.get('TYPE', '')
    df['credit_debit'] = df.get('CREDIT/DEBIT', '')

    # Create separate credit/debit amounts
    df['credit_amount'] = df['amount'].where(df['amount'] > 0, 0)
    df['debit_amount'] = df['amount'].where(df['amount'] < 0, 0).abs()

    return df


def standardize_botw_df(botw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize BotW transaction dataframe format.

    Args:
        botw_df: Raw BotW transaction dataframe

    Returns:
        Standardized BotW dataframe with consistent column names and types
    """
    df = botw_df.copy()

    # Standardize column names
    df.columns = df.columns.str.strip()

    # Convert Date to datetime (same calendar-day normalization as BMO / daily balance)
    if 'Date' in df.columns:
        df['transaction_date'] = _parse_to_normalized_calendar_dates(df['Date'])
    else:
        raise ValueError("BotW dataframe missing 'Date' column")

    # Handle Debit and Credit columns (may have $ signs)
    if 'Debit' in df.columns:
        df['debit_amount'] = df['Debit'].astype(str).str.replace('$', '').str.replace(',', '')
        df['debit_amount'] = pd.to_numeric(df['debit_amount'], errors='coerce').abs().fillna(0)
    else:
        df['debit_amount'] = 0

    if 'Credit' in df.columns:
        df['credit_amount'] = df['Credit'].astype(str).str.replace('$', '').str.replace(',', '')
        df['credit_amount'] = pd.to_numeric(df['credit_amount'], errors='coerce').fillna(0)
    else:
        df['credit_amount'] = 0

    # Calculate net amount (credit - debit)
    df['amount'] = df['credit_amount'] - df['debit_amount']

    # Create standardized columns
    df['bank_source'] = 'BotW'
    df['description'] = df.get('Description', '')
    df['currency'] = 'USD'  # BotW appears to be USD only
    df['transaction_reference'] = df.get('Check_No', '').astype(str)
    df['transaction_type'] = df.get('Type', '')
    df['credit_debit'] = df['transaction_type']

    return df


def join_bank_transactions(bmo_df: pd.DataFrame, botw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Join BMO and BotW transaction dataframes into a unified format.

    Args:
        bmo_df: BMO transaction dataframe
        botw_df: BotW transaction dataframe

    Returns:
        Combined dataframe with standardized columns
    """
    # Standardize both dataframes
    bmo_std = standardize_bmo_df(bmo_df)
    botw_std = standardize_botw_df(botw_df)

    # Select common columns for the combined dataframe
    common_columns = [
        'transaction_date',
        'amount',
        'credit_amount',
        'debit_amount',
        'bank_source',
        'description',
        'currency',
        'transaction_reference',
        'transaction_type',
        'credit_debit',
        'raw_source_file',
        'raw_source_file_created_date',
        'input_balance',
        'running_balance',
    ]

    # Ensure all columns exist in both dataframes
    for col in common_columns:
        if col not in bmo_std.columns:
            bmo_std[col] = None
        if col not in botw_std.columns:
            botw_std[col] = None

    # Combine the dataframes
    combined_df = pd.concat(
        [bmo_std[common_columns], botw_std[common_columns]],
        ignore_index=True,
        sort=False
    )

    # Sort by transaction date
    combined_df = combined_df.sort_values('transaction_date', na_position='last').reset_index(drop=True)

    return combined_df


def check_data_quality(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Perform comprehensive data quality checks on bank transaction dataframe.

    Args:
        df: Combined bank transaction dataframe

    Returns:
        Dictionary containing data quality check results
    """
    results = {
        'total_records': len(df),
        'date_checks': {},
        'amount_checks': {},
        'completeness_checks': {},
        'duplicate_checks': {},
        'consistency_checks': {},
        'warnings': [],
        'errors': [],
    }

    # Date quality checks
    null_dates = df['transaction_date'].isna().sum()
    results['date_checks']['null_dates'] = null_dates
    results['date_checks']['null_date_pct'] = (null_dates / len(df) * 100) if len(df) > 0 else 0

    if null_dates > 0:
        results['warnings'].append(f"{null_dates} records ({results['date_checks']['null_date_pct']:.2f}%) have null transaction dates")

    # Date range check
    valid_dates = df['transaction_date'].dropna()
    if len(valid_dates) > 0:
        results['date_checks']['min_date'] = valid_dates.min()
        results['date_checks']['max_date'] = valid_dates.max()
        results['date_checks']['date_range_days'] = (valid_dates.max() - valid_dates.min()).days

    # Amount quality checks
    null_amounts = df['amount'].isna().sum()
    results['amount_checks']['null_amounts'] = null_amounts
    results['amount_checks']['null_amount_pct'] = (null_amounts / len(df) * 100) if len(df) > 0 else 0

    if null_amounts > 0:
        results['warnings'].append(f"{null_amounts} records ({results['amount_checks']['null_amount_pct']:.2f}%) have null amounts")

    # Amount statistics
    valid_amounts = df['amount'].dropna()
    if len(valid_amounts) > 0:
        results['amount_checks']['min_amount'] = float(valid_amounts.min())
        results['amount_checks']['max_amount'] = float(valid_amounts.max())
        results['amount_checks']['mean_amount'] = float(valid_amounts.mean())
        results['amount_checks']['total_amount'] = float(valid_amounts.sum())
        results['amount_checks']['zero_amount_count'] = int((valid_amounts == 0).sum())

    # Completeness checks
    required_columns = ['transaction_date', 'amount', 'bank_source', 'description']
    for col in required_columns:
        null_count = df[col].isna().sum() if col in df.columns else len(df)
        results['completeness_checks'][col] = {
            'null_count': null_count,
            'null_pct': (null_count / len(df) * 100) if len(df) > 0 else 0,
            'completeness_pct': (1 - null_count / len(df)) * 100 if len(df) > 0 else 0
        }

    # Duplicate checks
    duplicate_rows = df.duplicated().sum()
    results['duplicate_checks']['duplicate_rows'] = duplicate_rows
    results['duplicate_checks']['duplicate_pct'] = (duplicate_rows / len(df) * 100) if len(df) > 0 else 0

    if duplicate_rows > 0:
        results['warnings'].append(f"{duplicate_rows} duplicate rows found")
        # Capture one random example set of duplicate records
        duplicated_mask = df.duplicated(keep=False)  # Mark all duplicates (not just second occurrence)
        if duplicated_mask.any():
            # Get all duplicate rows
            duplicate_rows_df = df[duplicated_mask].copy()
            
            # Group by all columns to find duplicate sets (rows with identical values)
            # This creates groups where each group contains rows that are duplicates of each other
            grouped = duplicate_rows_df.groupby(list(duplicate_rows_df.columns), dropna=False)
            
            # Get all groups that have more than 1 row (actual duplicate sets)
            duplicate_groups = [group for name, group in grouped if len(group) > 1]
            
            # Randomly select one duplicate set
            if len(duplicate_groups) > 0:
                random_group = random.choice(duplicate_groups)
                
                # Get up to 2 rows from this duplicate set
                duplicate_example = random_group.head(2)
                
                # Convert to dict for easier printing (store as list of dicts)
                results['duplicate_checks']['example_duplicates'] = duplicate_example.to_dict('records')

    # Check for duplicate transaction references within same bank
    if 'transaction_reference' in df.columns:
        dup_refs = df.groupby(['bank_source', 'transaction_reference']).size()
        dup_refs = dup_refs[dup_refs > 1]
        results['duplicate_checks']['duplicate_references'] = len(dup_refs)
        if len(dup_refs) > 0:
            results['warnings'].append(f"{len(dup_refs)} duplicate transaction references found")

    # Consistency checks
    # Check that credit/debit amounts are consistent with net amount
    if 'credit_amount' in df.columns and 'debit_amount' in df.columns:
        calculated_amount = df['credit_amount'] - df['debit_amount']
        amount_mismatch = (abs(calculated_amount - df['amount']) > 0.01).sum()
        results['consistency_checks']['amount_mismatch'] = amount_mismatch
        if amount_mismatch > 0:
            results['warnings'].append(f"{amount_mismatch} records have inconsistent credit/debit/amount values")

    # Bank source distribution
    if 'bank_source' in df.columns:
        bank_counts = df['bank_source'].value_counts().to_dict()
        results['consistency_checks']['bank_source_distribution'] = bank_counts

    # Currency check
    if 'currency' in df.columns:
        currency_counts = df['currency'].value_counts().to_dict()
        results['consistency_checks']['currency_distribution'] = currency_counts

    return results


def deduplicate_transactions(df: pd.DataFrame, keep: str = 'first') -> Tuple[pd.DataFrame, int]:
    """
    Remove duplicate rows from the transaction dataframe.

    Args:
        df: Combined bank transaction dataframe
        keep: Which duplicates to keep - 'first', 'last', or False (drop all)

    Returns:
        Tuple of (deduplicated_dataframe, number_of_duplicates_removed)
    """
    initial_count = len(df)
    
    # Remove duplicates, keeping the first occurrence by default
    deduplicated_df = df.drop_duplicates(keep=keep).reset_index(drop=True)
    
    duplicates_removed = initial_count - len(deduplicated_df)
    
    return deduplicated_df, duplicates_removed


def load_bmo_daily_balance_merge_table(
    raw_folder: Path,
    file_name: str = "bmoDailyBalance.csv",
) -> Optional[pd.DataFrame]:
    """
    Load ``bmoDailyBalance.csv`` and return a deduplicated merge table:
    ``(posted date, ref) -> input_balance``.

    The ``ref`` column must match the **transaction description** in the combined file
    (BMO ``DESCRIPTION`` / BotW ``Description``, standardized as ``description``), on
    the same **calendar date** as the balance row's ``date``. Dates are parsed with the
    same rules as transaction posted dates (``_parse_to_normalized_calendar_dates``):
    mixed string formats, optional Excel serial numbers, then normalized to midnight
    naive datetimes for an exact merge key match.
    """
    path = Path(raw_folder) / file_name
    if not path.is_file():
        return None

    df = pd.read_csv(path, encoding="utf-8-sig")
    df.columns = [str(c).strip() for c in df.columns]
    lower = {c.lower(): c for c in df.columns}

    def col(*candidates: str) -> Optional[str]:
        for name in candidates:
            if name.lower() in lower:
                return lower[name.lower()]
        return None

    date_c = col("date", "posted date", "post date", "transaction date")
    bal_c = col("balance", "running balance", "ledger balance", "ending balance")
    ref_c = col("ref", "reference")
    if not date_c or not bal_c or not ref_c:
        raise ValueError(
            f"{path.name} must include date, balance, and ref (or reference) columns "
            f"(found columns: {list(df.columns)})"
        )

    key = df[ref_c].astype(str).str.strip()
    key = key.mask(key.str.lower().eq("nan"), "")

    out = pd.DataFrame(
        {
            "_bal_merge_date": _parse_to_normalized_calendar_dates(df[date_c]),
            "_bal_merge_key": key,
            "input_balance": pd.to_numeric(df[bal_c], errors="coerce"),
        }
    )
    out = out.dropna(subset=["_bal_merge_date"])
    out = out[out["_bal_merge_key"].ne("")]
    out = out.drop_duplicates(subset=["_bal_merge_date", "_bal_merge_key"], keep="last")
    return out


def apply_bmo_daily_balance_running_balance(
    combined_df: pd.DataFrame,
    raw_folder: Path,
    daily_balance_file: str = "bmoDailyBalance.csv",
) -> pd.DataFrame:
    """
    After stack + dedupe: left-join ``input_balance`` from daily balance on
    ``(transaction_date, description) == (date, ref)`` for **all** banks, then compute
    ``running_balance`` in global sort order (date, bank_source, row key).
    """
    # This function now only reads the CSV and returns it as a DataFrame.
    path = Path(raw_folder) / daily_balance_file
    if not path.is_file():
        raise FileNotFoundError(f"{path} does not exist.")
    return pd.read_csv(path, encoding="utf-8-sig")


def calculate_monthly_transaction_counts(df: pd.DataFrame) -> Dict[str, int]:
    """
    Calculate monthly transaction counts from the dataframe.

    Args:
        df: Bank transaction dataframe (should be deduplicated)

    Returns:
        Dictionary mapping month-year strings to transaction counts
    """
    valid_dates = df['transaction_date'].dropna()
    if len(valid_dates) == 0:
        return {}
    
    # Create month-year string (e.g., "2023-01", "2024-12")
    df_with_dates = df[df['transaction_date'].notna()].copy()
    df_with_dates['month_year'] = df_with_dates['transaction_date'].dt.to_period('M').astype(str)
    monthly_counts = df_with_dates['month_year'].value_counts().sort_index().to_dict()
    
    return monthly_counts


def process_bank_transactions(
    bmo_df: pd.DataFrame,
    botw_df: pd.DataFrame,
    perform_dq_checks: bool = True,
    verbose: bool = True,
    deduplicate: bool = True,
    deduplicate_keep: str = 'first',
    raw_folder: Optional[Path] = None,
    bmo_daily_balance_file: str = "bmoDailyBalance.csv",
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Main function to join bank transactions and perform data quality checks.

    Args:
        bmo_df: BMO transaction dataframe
        botw_df: BotW transaction dataframe
        perform_dq_checks: Whether to perform data quality checks
        verbose: Whether to print DQ check results
        deduplicate: Whether to remove duplicate rows (default: True)
        deduplicate_keep: Which duplicates to keep - 'first', 'last', or False (drop all)
        raw_folder: If set, load ``bmo_daily_balance_file`` from this folder after dedupe
            to add ``input_balance`` (join) and ``running_balance`` (all rows).
        bmo_daily_balance_file: CSV name under ``raw_folder`` (default: bmoDailyBalance.csv).

    Returns:
        Tuple of (combined_dataframe, dq_results_dict)
    """
    # Join the dataframes
    combined_df = join_bank_transactions(bmo_df, botw_df)

    # Perform data quality checks (before deduplication to see original state)
    dq_results = {}
    if perform_dq_checks:
        dq_results = check_data_quality(combined_df)

    # Deduplicate if requested
    duplicates_removed = 0
    if deduplicate:
        deduped_df = deduplicate_transactions_with_tracing(combined_df, trace=True)
        duplicates_removed = len(combined_df) - len(deduped_df)
        combined_df = deduped_df
        dq_results['deduplication'] = {
            'duplicates_removed': duplicates_removed,
            'deduplication_applied': True,
            'keep_strategy': 'oldest_by_file_creation',
            'final_record_count': len(combined_df)
        }
    else:
        dq_results['deduplication'] = {
            'duplicates_removed': 0,
            'deduplication_applied': False,
            'final_record_count': len(combined_df)
        }

    # Update total records count after deduplication
    if 'deduplication' in dq_results:
        dq_results['total_records_after_dedup'] = len(combined_df)

    if raw_folder is not None:
        bal_path = Path(raw_folder) / bmo_daily_balance_file
        bmo_daily_balance_df = apply_bmo_daily_balance_running_balance(
            combined_df,  # argument is ignored in new version
            Path(raw_folder),
            daily_balance_file=bmo_daily_balance_file,
        )
        dq_results["bmo_daily_balance"] = {
            "applied": bal_path.is_file(),
            "raw_folder": str(raw_folder),
            "file": bmo_daily_balance_file,
            "row_count": len(bmo_daily_balance_df) if bal_path.is_file() else 0,
        }
    else:
        dq_results["bmo_daily_balance"] = {"applied": False}

    # Calculate monthly transaction counts on deduplicated dataframe
    monthly_counts = calculate_monthly_transaction_counts(combined_df)
    dq_results['monthly_transaction_counts'] = monthly_counts

    if verbose:
            print("=" * 60)
            print("BANK TRANSACTION DATA QUALITY REPORT")
            print("=" * 60)
            print(f"\nTotal Records (before deduplication): {dq_results['total_records']:,}")
            
            # Show deduplication results
            if 'deduplication' in dq_results and dq_results['deduplication']['deduplication_applied']:
                dup_info = dq_results['deduplication']
                print(f"Duplicates Removed: {dup_info['duplicates_removed']:,}")
                print(f"Total Records (after deduplication): {dup_info['final_record_count']:,}")
            else:
                print(f"Total Records (no deduplication): {dq_results['total_records']:,}")

            # Print monthly transaction counts (after deduplication)
            if 'monthly_transaction_counts' in dq_results and dq_results['monthly_transaction_counts']:
                print("\nTransactions per Month (after deduplication):")
                for month_year, count in dq_results['monthly_transaction_counts'].items():
                    print(f"  {month_year}: {count:,}")

            print("\n--- Date Quality ---")
            print(f"Null Dates: {dq_results['date_checks'].get('null_dates', 0)}")
            if 'min_date' in dq_results['date_checks']:
                print(f"Date Range: {dq_results['date_checks']['min_date']} to {dq_results['date_checks']['max_date']}")
                print(f"Range (days): {dq_results['date_checks'].get('date_range_days', 0):,}")

            print("\n--- Amount Quality ---")
            print(f"Null Amounts: {dq_results['amount_checks'].get('null_amounts', 0)}")
            if 'total_amount' in dq_results['amount_checks']:
                print(f"Total Amount: ${dq_results['amount_checks']['total_amount']:,.2f}")
                print(f"Mean Amount: ${dq_results['amount_checks'].get('mean_amount', 0):,.2f}")
                print(f"Zero Amount Records: {dq_results['amount_checks'].get('zero_amount_count', 0)}")

            print("\n--- Completeness ---")
            for col, stats in dq_results['completeness_checks'].items():
                print(f"{col}: {stats['completeness_pct']:.2f}% complete ({stats['null_count']} nulls)")

            print("\n--- Duplicates ---")
            print(f"Duplicate Rows (before deduplication): {dq_results['duplicate_checks'].get('duplicate_rows', 0)}")
            print(f"Duplicate References: {dq_results['duplicate_checks'].get('duplicate_references', 0)}")
            
            # Show deduplication status
            if 'deduplication' in dq_results:
                dup_info = dq_results['deduplication']
                if dup_info['deduplication_applied']:
                    print(f"Deduplication Applied: Yes (removed {dup_info['duplicates_removed']:,} duplicates, keep='{dup_info['keep_strategy']}')")
                else:
                    print("Deduplication Applied: No")
            
            # Print example duplicate records if they exist (these are from before deduplication)
            if 'example_duplicates' in dq_results['duplicate_checks']:
                print("\nExample Duplicate Records (before deduplication):")
                for idx, dup_record in enumerate(dq_results['duplicate_checks']['example_duplicates'], 1):
                    print(f"  Record {idx}:")
                    for key, value in dup_record.items():
                        print(f"    {key}: {value}")

            if 'bank_source_distribution' in dq_results['consistency_checks']:
                print("\n--- Bank Source Distribution ---")
                for bank, count in dq_results['consistency_checks']['bank_source_distribution'].items():
                    print(f"{bank}: {count:,} records")

            if dq_results['warnings']:
                print("\n--- WARNINGS ---")
                for warning in dq_results['warnings']:
                    print(f"  ⚠ {warning}")

            if dq_results['errors']:
                print("\n--- ERRORS ---")
                for error in dq_results['errors']:
                    print(f"  ✗ {error}")

            bal_info = dq_results.get("bmo_daily_balance", {})
            if bal_info.get("raw_folder"):
                print("\n--- BMO daily balance (input_balance / running_balance) ---")
                if bal_info.get("applied"):
                    print(f"  Rows with input_balance (date + description == date + ref): {bal_info.get('matched_rows', 0):,}")
                else:
                    print(f"  Skipped (file not found: {bal_info.get('file', '')})")

            print("\n" + "=" * 60)

    return combined_df, dq_results

