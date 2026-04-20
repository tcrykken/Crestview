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


## Transaction Categorization Integration

The pipeline now automatically categorizes all bank transactions using the latest crosswalks before exporting `combined_bank_transactions.csv`. The `category` column is included in the output.

- Crosswalks are loaded and combined automatically.
- The crosswalk tool can still be used independently to update or export crosswalks (see below).
- To interactively categorize unknown patterns, use the crosswalk tool as before.

**No extra steps are needed to ensure categorized output in the main pipeline.**

### Running the Crosswalk Tool (Advanced/Interactive)

You can still use the crosswalk tool for interactive or programmatic management of crosswalks and unknown patterns. See the [Transaction Categorization / Crosswalk Tool](#) section in previous versions for details.

``` bash
python -i -c "
import pandas as pd
from rental_analytics.data_access.transaction_categorization import combine_crosswalks, categorize_transactions_df, interactively_categorize_unknowns, export_finalized_crosswalk
df = pd.read_csv('/Users/a2338-home/Documents/Crestview/305Analysis/data/Staging/combined_bank_transactions.csv')
crosswalks_df = combine_crosswalks()
categorized_df = categorize_transactions_df(df, crosswalks_df=crosswalks_df)
updated_crosswalk = interactively_categorize_unknowns(categorized_df, crosswalks_df=crosswalks_df, auto_save=True, show_existing_categories=True)
export_finalized_crosswalk(updated_crosswalk, include_bank_source=True)
"
```

### Output Location

- The finalized crosswalk CSV is exported to:
  - `data/output/finalized_crosswalk.csv`
- The categorized combined bank transactions are exported to:
  - `data/Staging/combined_bank_transactions.csv`

The output directories are created automatically if they don't exist.

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
2. **Processes and combines** bank transactions (standardizes, deduplicates, performs DQ checks), then—if `data/Raw/bmoDailyBalance.csv` exists—joins **input_balance** and computes **running_balance** (see below)
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

### Debugging load for raw bank files example zsh command
``` bash
python3 -c "
from pathlib import Path
from rental_analytics.data_access.loaders import load_raw_bank_files
load_raw_bank_files()
```

### Combined bank staging: `input_balance` and `running_balance`

After BMO and BotW raw files are stacked and **deduplicated**, the pipeline optionally enriches `combined_bank_transactions.csv` using `data/Raw/bmoDailyBalance.csv`.

**`bmoDailyBalance.csv` (required columns)**

- **date** — posted date for the row (column name can also be `posted date`, `post date`, or `transaction date`).
- **balance** — ledger balance for that line (aliases: `running balance`, `ledger balance`, `ending balance`).
- **ref** — must match the **exact transaction description** after standardization (`description` in the combined file): i.e. BMO export **DESCRIPTION** or BotW **Description**. A fallback header **reference** is accepted. This is **not** the BMO “FI TRANSACTION REFERENCE” column; that field is not used for this join.

**Join rule**

- Match on **calendar date** (normalized) **and** stripped string equality: `transaction_date` (date part) + `description` = `date` + `ref`.
- Applies to **all rows** (BMO and BotW). Any row whose description and date appear in the daily balance file gets **input_balance** populated.

**Date alignment (BMO export vs `bmoDailyBalance`)**

- Both sides use the same helper: `format="mixed"` parsing, then **midnight-normalized** naive datetimes so only the calendar day is compared.
- BMO **POSTED DATE** values like `1/5/2026` (no leading zeros) are supported; the old strict `MM/DD/YYYY` parser could turn those into null dates and break the join even when `ref` matched.
- Daily balance `date` cells can be ISO (`2026-01-05`), `M/D/YYYY`, or **numeric Excel serial days**; the file is read with `utf-8-sig` so a BOM does not corrupt the first header.
- If either side is timezone-aware, it is converted to UTC, normalized to a calendar day, then stored without timezone so merge keys line up.

**`running_balance`**

- Computed on the **full** deduplicated combined dataframe, sorted by `transaction_date`, then `bank_source`, then original row order.
- Rows with **input_balance** anchor the chain (balance after that transaction, consistent with the daily balance export). Rows without a match extend the chain with **amount**; rows before the first anchor are back-filled from that anchor when possible.

If `bmoDailyBalance.csv` is missing, `input_balance` and `running_balance` are still written as columns filled with NaN so the staging schema stays stable.

**Example zsh commands**

Rebuild the combined bank file and print sample columns (adjust `root` if your clone lives elsewhere):

```bash
cd /Users/a2338-home/Documents/Crestview/305Analysis/python
source venv/bin/activate
python3 -c "
from pathlib import Path
from rental_analytics.data_access.loaders import load_raw_bank_files
from rental_analytics.data_access.bank_transactions import process_bank_transactions

root = Path('/Users/a2338-home/Documents/Crestview/305Analysis')
raw = root / 'data' / 'Raw'
bmo, botw = load_raw_bank_files()
df, dq = process_bank_transactions(
    bmo, botw,
    perform_dq_checks=False,
    verbose=True,
    raw_folder=raw,
)
print(df[['transaction_date', 'bank_source', 'description', 'amount', 'input_balance', 'running_balance']].head(20))
print('matched input_balance rows:', df['input_balance'].notna().sum())
out = root / 'data' / 'Staging' / 'combined_bank_transactions.csv'
df.to_csv(out, index=False)
print('wrote', out)
"
```

Quick check that the staging file has the new columns:

```bash
head -1 /Users/a2338-home/Documents/Crestview/305Analysis/data/Staging/combined_bank_transactions.csv
```

Run the full P&L pipeline (same staging write + Airbnb loads + `run_pnl`):

```bash
cd /Users/a2338-home/Documents/Crestview/305Analysis/python
source venv/bin/activate
python3 -m rental_analytics.pipelines.pnl_pipeline
```

**Running the Pipeline:**

1. import and call run() directly
```bash
python3 -c "from rental_analytics.pipelines.pnl_pipeline import run; run()"
```
2. run as a module
```bash
python3 -m rental_analytics.pipelines.pnl_pipeline
```
3. 
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