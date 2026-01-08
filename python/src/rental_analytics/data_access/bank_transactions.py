"""Bank transaction data processing and data quality checks.

This module handles joining BMO and BotW bank transaction dataframes,
standardizing their formats, and performing data quality checks.
"""

import pandas as pd
from typing import Tuple, Dict, List
import warnings


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

    # Convert POSTED DATE to datetime
    if 'POSTED DATE' in df.columns:
        df['transaction_date'] = pd.to_datetime(df['POSTED DATE'], format='%m/%d/%Y', errors='coerce')
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

    # Convert Date to datetime
    if 'Date' in df.columns:
        df['transaction_date'] = pd.to_datetime(df['Date'], errors='coerce')
    else:
        raise ValueError("BotW dataframe missing 'Date' column")

    # Handle Debit and Credit columns (may have $ signs)
    if 'Debit' in df.columns:
        df['debit_amount'] = df['Debit'].astype(str).str.replace('$', '').str.replace(',', '')
        df['debit_amount'] = pd.to_numeric(df['debit_amount'], errors='coerce').fillna(0)
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


def check_data_quality(df: pd.DataFrame) -> Dict[str, any]:
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


def process_bank_transactions(
    bmo_df: pd.DataFrame,
    botw_df: pd.DataFrame,
    perform_dq_checks: bool = True,
    verbose: bool = True
) -> Tuple[pd.DataFrame, Dict[str, any]]:
    """
    Main function to join bank transactions and perform data quality checks.

    Args:
        bmo_df: BMO transaction dataframe
        botw_df: BotW transaction dataframe
        perform_dq_checks: Whether to perform data quality checks
        verbose: Whether to print DQ check results

    Returns:
        Tuple of (combined_dataframe, dq_results_dict)
    """
    # Join the dataframes
    combined_df = join_bank_transactions(bmo_df, botw_df)

    # Perform data quality checks
    dq_results = {}
    if perform_dq_checks:
        dq_results = check_data_quality(combined_df)

        if verbose:
            print("=" * 60)
            print("BANK TRANSACTION DATA QUALITY REPORT")
            print("=" * 60)
            print(f"\nTotal Records: {dq_results['total_records']:,}")

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
            print(f"Duplicate Rows: {dq_results['duplicate_checks'].get('duplicate_rows', 0)}")
            print(f"Duplicate References: {dq_results['duplicate_checks'].get('duplicate_references', 0)}")

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

            print("\n" + "=" * 60)

    return combined_df, dq_results

