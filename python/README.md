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