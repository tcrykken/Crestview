# Crestview 

Multi-language data processing pipeline combining SAS, Python, and Go.

## Architecture
```
Python Extract → SAS Transform → Go Process → SAS Analytics
```

## Setup

### Prerequisites
- SAS 9.4+
- Python 3.9+
- Go 1.21+

### Installation
```bash
# Python dependencies
cd python
pip install -r requirements.txt

# Go dependencies
cd go
go mod download

# Build Go executable
go build -o bin/processor ./cmd/processor
```

## Usage
```bash
# Run full pipeline
./scripts/run_pipeline.sh (DNE)

# Run individual components
python python/src/...
sas sas/...
./go/bin/etl-processor -input=data/staging/...export.json -output=data/staging/go_result...
```

## Project Structure

- `sas/` - SAS data processing
- `python/` - Data extraction
- `go/` - Data joins and transformations
- `data/` - Local data files (gitignored)
- `docs/` - Documentation