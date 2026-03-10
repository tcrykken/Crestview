from __future__ import annotations

from typing import Iterable, Mapping, Any

import pandas as pd


def normalize_date_columns(
    df: pd.DataFrame,
    date_cols,
    *,
    dayfirst: bool | None = None,
    yearfirst: bool | None = True,
    errors: str = "coerce",
) -> pd.DataFrame:
    """
    Convert specified columns from mixed string formats to standardized datetime.

    Args:
        df: DataFrame whose columns should be converted in-place.
        date_cols: Iterable of column names expected to contain date-like values.
        dayfirst: Whether to interpret the first value as the day (e.g. 01/02/2018 -> 1 Feb 2018).
        yearfirst: Whether to interpret the first value as the year when ambiguous.
        errors: Passed through to ``pd.to_datetime`` (\"coerce\", \"raise\", or \"ignore\").
        infer_datetime_format: Hint to pandas to infer formats for performance and robustness.

    Returns:
        The same DataFrame instance with normalized date columns.
    """
    for col in date_cols:
        if col not in df.columns:
            continue

        df[col] = pd.to_datetime(
            df[col],
            errors=errors,
            dayfirst=dayfirst,
            yearfirst=yearfirst,
            format="mixed",  # keep this if you want mixed formats
        )

    return df


def normalize_date_columns_from_config(
    df: pd.DataFrame, 
    *, 
    config
    ) -> pd.DataFrame:
 
    """
    Convenience wrapper to normalize date columns using a small config dict.

    Example ``config``:

        {
            "cols": ["Start Date", "End Date"],
            "dayfirst": False,
            "yearfirst": True,
            "errors": "coerce",
        }
    """
    cols = config.get("cols", [])
    if not cols:
        return df

    return normalize_date_columns(
        df,
        cols,
        dayfirst=config.get("dayfirst"),
        yearfirst=config.get("yearfirst", True),
        errors=config.get("errors", "coerce"),
    )

