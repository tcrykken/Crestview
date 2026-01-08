"""Analytics and financial analysis for a short-term rental business."""

__version__ = "0.1.0"

# Main public API - expose commonly used functions/classes
from rental_analytics.data_access.loaders import load_rental_data
from rental_analytics.finance.pnl import run_pnl
from rental_analytics.pipelines.pnl_pipeline import run as run_pipeline

__all__ = [
    "load_rental_data",
    "run_pnl",
    "run_pipeline",
]

