# Singapore Government Directory

A data pipeline that extracts, transforms, and loads information from the [Singapore Government Directory (SGDI)](https://www.sgdi.gov.sg/) into BigQuery for analysis.

## Overview

This project consists of two main components:

1. **Extractor** - Python scripts that scrape data from SGDI and load it into BigQuery
2. **DBT** - Data transformation models that process the raw data into analytical tables

## Project Structure

```
├── extractor/              # Data extraction pipeline
│   ├── main.py             # Main entry point
│   ├── ministries/         # Ministry & Organs of State extraction
│   ├── name_cleaning/      # Name standardization pipeline
│   ├── slowly_changing_dimensions/  # SCD processing
│   ├── utils/              # HTML downloading & parsing utilities
│   ├── gbq/                # BigQuery utilities
│   └── token/              # GCP credentials (gitignored)
├── dbt/                    # DBT transformation models
│   ├── models/
│   │   ├── staging/        # Raw data staging
│   │   ├── dimensions/     # Dimension tables
│   │   ├── facts/          # Fact tables
│   │   └── marts/          # Business-level aggregations
│   └── ...
└── requirements.txt        # Legacy requirements (use pyproject.toml instead)
```

## Prerequisites

- Python 3.10+
- [uv](https://github.com/astral-sh/uv) (recommended) or pip
- Google Cloud Platform account with BigQuery access
- GCP Service Account JSON key

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/jeremychia/singapore-government-directory.git
cd singapore-government-directory
```

### 2. Set up GCP credentials

Place your GCP service account JSON file at:

```
extractor/token/gcp_token.json
```

The service account needs the following permissions:
- BigQuery Data Editor
- BigQuery Job User

### 3. Install dependencies

Using uv (recommended):

```bash
cd extractor
uv sync
```

Or using pip:

```bash
cd extractor
pip install -e .
```

## Usage

### Pre-flight Checks

Before running any extraction, validate that all requirements are met:

```bash
cd extractor
uv run python main.py --check
```

### Extract Ministry Data

Extract data from all ministries:

```bash
uv run python main.py --ministry_extractor
```

Extract from a specific ministry:

```bash
uv run python main.py --ministry_extractor --ministry "Ministry of Education"
```

### Extract Organs of State Data

Extract data from all organs of state:

```bash
uv run python main.py --organs_of_state_extractor
```

### Process Slowly Changing Dimensions

Convert raw data into SCD format:

```bash
uv run python main.py --slowly_changing_dimensions
```

### Run Name Cleaning

Standardize and clean name data:

```bash
uv run python main.py --name_cleaning
```

### Resume a Failed Run

If extraction breaks midway, resume from a specific ministry:

```bash
uv run python main.py --ministry_extractor --ministry "Ministry of Health" --resume_run
```

### Command Line Options

| Flag | Short | Description |
|------|-------|-------------|
| `--ministry_extractor` | `-me` | Run ministry data extraction |
| `--ministry` | `-m` | Specify ministry name(s) to extract |
| `--organs_of_state_extractor` | `-oose` | Run organs of state extraction |
| `--organs_of_state` | `-oos` | Specify organ(s) of state to extract |
| `--slowly_changing_dimensions` | `-scd` | Process SCD transformations |
| `--name_cleaning` | `-nc` | Run name cleaning pipeline |
| `--resume_run` | `-rr` | Resume from specified ministry/organ |
| `--check` | `-c` | Run pre-flight checks only |
| `--skip_checks` | | Skip pre-flight checks (not recommended) |

## Data Sources

### Ministries

- Ministry of Culture, Community and Youth
- Ministry of Defence
- Ministry of Digital Development and Information
- Ministry of Education
- Ministry of Finance
- Ministry of Foreign Affairs
- Ministry of Health
- Ministry of Home Affairs
- Ministry of Law
- Ministry of Manpower
- Ministry of National Development
- Ministry of Social and Family Development
- Ministry of Sustainability and the Environment
- Ministry of Trade and Industry
- Ministry of Transport
- Prime Minister's Office

### Organs of State

- Attorney-General's Chambers
- Auditor-General's Office
- Industrial Arbitration Court
- Istana
- Judiciary (Family Justice Courts, State Courts, Supreme Court)
- Parliament of Singapore
- Public Service Commission
- The Cabinet

## DBT Models

To run DBT transformations:

```bash
cd dbt
poetry install
poetry run dbt run
```

## License

This project is for educational and research purposes. Data sourced from [SGDI](https://www.sgdi.gov.sg/) is subject to Singapore Government Terms of Use.
