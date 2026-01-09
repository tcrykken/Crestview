# Rental Analytics

Analytics and financial analysis for a short-term rental business.

## Setup

### 1. Create a Virtual Environment

Navigate to the `python` directory and create a virtual environment:

```bash
cd python
python3 -m venv venv
```

Or if you're already in the python directory:

```bash
python3 -m venv venv
```

### 2. Activate the Virtual Environment

**On macOS/Linux:**
```bash
source venv/bin/activate
```

**On Windows:**
```bash
venv\Scripts\activate
```

### 3. Install the Project

Once the virtual environment is activated, install the project in editable mode:

```bash
pip install -e .
```

This will install the project along with its dependencies (pandas, numpy).

### 4. Install Development Dependencies (Optional)

To install development dependencies (pytest, ipython):

```bash
pip install -e ".[dev]"
```

### 5. Deactivate the Virtual Environment

When you're done working, deactivate the virtual environment:

```bash
deactivate
```

## Requirements

- Python >= 3.9.6

## Project Structure

- `src/rental_analytics/` - Main package code
- `tests/` - Test files
- `notebooks/` - Jupyter notebooks for analysis

## Testing

- Pytest is in your dev dependencies. Run the test with:

- - From the python directory (with venv activated):
``` bash
pytest tests/pipelines/test_pnl_pipeline.py
```

- - Or from the project root (305Analysis):
``` bash
pytest python/tests/pipelines/test_pnl_pipeline.py
```

- - If you haven't installed dev dependencies yet:
``` bash
pip install -e ".[dev]"
```

- Other useful pytest options:

- - Run with verbose output: 
``` bash
pytest -v tests/pipelines/test_pnl_pipeline.py
```

- - Show print statements: 
``` bash
pytest -s tests/pipelines/test_pnl_pipeline.py
```

- - Run a specific test function: 
``` bash
pytest tests/pipelines/test_pnl_pipeline.py::test_run_pnl
```

- - Run all tests: 
``` bash
pytest (from the python directory)
```

## Transaction Categorization / Crosswalk Tool

The transaction categorization module provides tools to categorize bank transactions using crosswalk reference files and export finalized crosswalks.

### Features

- **Normalized Pattern Matching**: Automatically matches patterns regardless of spacing and case (e.g., "DIRECTDEBITCODEPTREVENUETAXPAYMENT" matches "Direct Debit COD EPT Revenue Tax Payment")
- **Multi-Bank Support**: Combines crosswalks from multiple banks (BMO, BotW)
- **Unknown Pattern Detection**: Identifies transactions without categories
- **Interactive Categorization**: Prompts user in terminal (zsh/bash/etc.) to categorize unknown patterns with options to assign, skip, or view existing categories
- **Auto-Save**: Automatically saves crosswalk after each categorization during interactive mode
- **CSV Export**: Exports finalized crosswalks to `data/output/`

### Running the Crosswalk Tool

#### Option 1: Run the Script Directly

From the `python` directory (with venv activated):

```bash
python src/rental_analytics/data_access/transaction_categorization.py
```

This will:
- Load all crosswalk files from `data/reference/`
- Combine them into a unified mapping
- Test normalized matching with sample descriptions
- Export a finalized crosswalk CSV to `data/output/finalized_crosswalk.csv`

#### Option 2: Interactive Categorization (Recommended for Unknown Patterns)

The interactive mode prompts you in the terminal (works in zsh, bash, or any shell) to categorize unknown patterns as they're encountered:

```python
from rental_analytics.data_access.transaction_categorization import (
    categorize_transactions_df,
    interactively_categorize_unknowns
)

# First, categorize transactions (this will leave some as None/unknown)
categorized_df = categorize_transactions_df(
    transactions_df,
    description_col='description',
    bank_source_col='bank_source'
)

# Then interactively categorize the unknowns
updated_crosswalk = interactively_categorize_unknowns(
    categorized_df,
    description_col='description',
    bank_source_col='bank_source',
    category_col='category',
    auto_save=True,  # Auto-saves after each assignment
    show_existing_categories=True  # Shows existing categories for reference
)
```

**Interactive Commands:**
- Enter a category name to assign it to the pattern
- Type `skip` or `s` to skip this pattern (leaves as UNKNOWN)
- Type `list` or `l` to see all existing categories
- Type `quit` or `q` to stop and save progress

The tool works in any shell (zsh, bash, etc.) using Python's built-in `input()` function - no special shell configuration needed.

#### Option 3: Use Programmatically (Non-Interactive)

Import and use the functions in your Python code:

```python
from rental_analytics.data_access.transaction_categorization import (
    combine_crosswalks,
    categorize_transactions_df,
    add_unknown_patterns,
    export_finalized_crosswalk
)

# Load and combine crosswalks
crosswalks_df = combine_crosswalks()

# Categorize transactions (with normalized matching)
categorized_df = categorize_transactions_df(
    transactions_df,
    description_col='description',
    bank_source_col='bank_source'
)

# Add unknown patterns to crosswalk (non-interactive, marks as UNKNOWN)
updated_crosswalk = add_unknown_patterns(
    categorized_df,
    description_col='description',
    category_col='category',
    crosswalks_df=crosswalks_df
)

# Export finalized crosswalk
export_path = export_finalized_crosswalk(
    updated_crosswalk,
    filename='finalized_crosswalk.csv',
    include_bank_source=True
)
print(f"Exported to: {export_path}")
```

### Output Location

The finalized crosswalk CSV is exported to:
```
data/output/finalized_crosswalk.csv
```

The output directory is created automatically if it doesn't exist.

### Crosswalk File Format

The tool expects crosswalk files in `data/reference/` with the following structure:
- **BMO**: `BMO 2024 tx cat xwalk.csv`
- **BotW**: `BotW desc cat xwalk.csv` and `BotW desc cat xwalk_2023Partnership.csv`

Each crosswalk file should have columns:
- `search_pattern`: The pattern to match (can be space-stripped, uppercase)
- `category`: The category to assign when pattern matches

The tool normalizes both patterns and transaction descriptions (removes spaces, converts to uppercase) for matching, so patterns like "DIRECTDEBITCODEPTREVENUETAXPAYMENT" will match descriptions with any spacing or case.