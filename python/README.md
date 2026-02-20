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

### 6. Deactivate the Virtual Environment

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

## Function Usage

To run an individual function from bash follow below form:
``` bash
python3 -c "
from pathlib import Path
import pandas as pd
from rental_analytics.data_access import load_union_ABBexp

df = load_union_ABBexp(
    local_raw_folder=Path('/Users/a2338-home/Documents/Crestview/305Analysis/data/Raw'),
    file_patterns=['airbnb_t*', 'airbnb_tax*']
)
print(df.head())
"

python3 -c "
from pathlib import Path
import pandas as pd
from rental_analytics.data_access import load_union_ABBres

df = load_union_ABBres(
    local_raw_folder=Path('/Users/a2338-home/Documents/Crestview/305Analysis/data/Raw'),
    file_patterns=['airbnb_res*', '*reservation*', '*rez*', '_ABnB_Res*', '_ABnb_Res*']
)
print(df.head())
"
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

## Google Drive Data Ingestion

The `raw_injestion` module provides functionality to download CSV extracts from Google Drive and stack them together.

### Setup

Install Google Drive support:

```bash
pip install gdown
# or
pip install -e ".[gdrive]"
```

### Usage

#### Download from a Google Drive Folder

```python
from rental_analytics.data_access.raw_injestion import load_union_ABBexp

# Download all files matching patterns from a folder
df = load_union_ABBexp(
    google_drive_folder_id='YOUR_FOLDER_ID',
    file_patterns=['airbnb_t*', 'airbnb_tax*'],
    local_cache_dir=Path('data/cache'),  # Optional: cache downloads
    use_cache=True  # Use cached files if available
)
```

#### Download Specific Files

```python
# Using file IDs
df = load_union_ABBexp(
    google_drive_file_ids=['FILE_ID_1', 'FILE_ID_2', 'FILE_ID_3']
)

# Using shareable links
from rental_analytics.data_access.raw_injestion import load_union_ABBexp_from_links

df = load_union_ABBexp_from_links([
    'https://drive.google.com/file/d/FILE_ID_1/view?usp=sharing',
    'https://drive.google.com/file/d/FILE_ID_2/view?usp=sharing'
])
```

### Getting Google Drive File/Folder IDs

1. **File ID**: Open the file in Google Drive, the ID is in the URL:
   - `https://drive.google.com/file/d/FILE_ID_HERE/view`
   
2. **Folder ID**: Open the folder in Google Drive, the ID is in the URL:
   - `https://drive.google.com/drive/folders/FOLDER_ID_HERE`

### Google Drive Authorization

The tool supports three authorization methods:

#### Option 1: Public Files (Easiest - Recommended)

**For files/folders shared publicly:**
1. Right-click the file/folder in Google Drive
2. Select "Share" → "Change to anyone with the link"
3. Set permission to "Viewer"
4. No authentication needed - the tool will download directly

**Pros:** Simple, no setup required  
**Cons:** Files are publicly accessible (use with caution for sensitive data)

#### Option 2: OAuth Authentication (Interactive)

**For private files/folders:**
- On first download, `gdown` will open your **system's default browser** and prompt you to sign in
- Grant access to Google Drive
- Authentication token is cached for future use

**⚠️ Important for Multiple Accounts:**
- `gdown` uses your **default browser**, not a specific profile
- **Before running**: Ensure the correct browser profile is set as your system default, OR
- **Log into the desired Google account** in your default browser before running the script
- The OAuth flow will use whichever account is active in that browser

**Workflow for Multiple Accounts:**
```bash
# Option A: Set default browser to specific profile first
# (macOS example - set Chrome profile as default)
# Then run your script
python -m rental_analytics.pipelines.pnl_pipeline

# Option B: Log into correct account in default browser, then run
# The OAuth will use the already-logged-in account
```

**Pros:** Works with private files, no code changes needed  
**Cons:** Requires interactive browser session, uses default browser (may not be desired profile)

#### Option 3: Service Account (For Automation)

**For automated/CI/CD workflows:**
1. Create a Google Cloud Project
2. Enable Google Drive API
3. Create a Service Account and download JSON key file
4. Share your Drive folder with the service account email
5. Use the service account file in your code

```python
from pathlib import Path

run(
    use_raw_injestion=True,
    gdrive_txhx_folder_id='YOUR_FOLDER_ID',
    use_service_account=True,
    service_account_file=Path('path/to/service-account-key.json')
)
```

**Pros:** Works for automated scripts, no user interaction, no browser profile issues  
**Cons:** Requires Google Cloud setup

**💡 Recommendation for Multiple Accounts:**
If you frequently switch between Google accounts, **service accounts are the best option** because:
- No browser profile confusion
- Each service account can access specific folders
- Works seamlessly in automated workflows
- No need to manage browser sessions

### Using the P&L Pipeline

The P&L pipeline follows this workflow:

1. **Loads raw bank files** from `data/Raw/` (BMO and BotW CSV files)
2. **Processes and combines** bank transactions (standardizes, deduplicates, performs DQ checks)
3. **Saves combined bank transactions** to `data/Staging/combined_bank_transactions.csv`
4. **Loads all staged files** from `data/Staging/`:
   - `ABB_stackres_00py.csv` (Airbnb reservations)
   - `ABB_stackTXHX_00py.csv` (Airbnb transaction history)
   - `combined_bank_transactions.csv` (processed bank transactions)
5. **Runs P&L analysis** on the staged data

**Data Directory Structure:**
- `data/Raw/` - Source files (individual bank CSVs, individual Airbnb extracts)
- `data/Staging/` - Processed/stacked files ready for analysis:
  - `ABB_stackres_00py.csv`
  - `ABB_stackTXHX_00py.csv`
  - `combined_bank_transactions.csv`

**Running the Pipeline:**

```python
from rental_analytics.pipelines.pnl_pipeline import run

run()
```

The pipeline will automatically:
- Process raw bank files and save to Staging
- Load all staged files
- Run the P&L analysis

### Features

- **Automatic Stacking**: Combines multiple CSV files into a single dataframe
- **Pattern Matching**: Filter files by name patterns (e.g., `airbnb_t*`)
- **Caching**: Optionally cache downloads to avoid re-downloading
- **Error Handling**: Continues processing if individual files fail to load
- **Column Standardization**: Automatically capitalizes column names (matching legacy behavior)
- **Dual Data Sources**: Supports both TXHX and reservation extracts