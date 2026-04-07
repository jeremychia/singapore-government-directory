# Singapore Government Directory

[![Deploy Docs](https://github.com/jeremychia/singapore-government-directory/actions/workflows/deploy-docs.yml/badge.svg)](https://github.com/jeremychia/singapore-government-directory/actions/workflows/deploy-docs.yml)

A data pipeline that extracts, transforms, and loads information from the [Singapore Government Directory (SGDI)](https://www.sgdi.gov.sg/) into BigQuery for analysis.

## 📊 Live Dashboard

View the analytics dashboard: **[jeremychia.github.io/singapore-government-directory](https://jeremychia.github.io/singapore-government-directory/)**

The dashboard includes:
- **Demographics Overview** - Gender and ethnicity distribution across the Singapore public service
- **Ministry Breakdown** - Demographics by ministry with interactive charts
- **Organs of State** - Separate analysis for constitutional bodies
- **Data Quality Metrics** - Completeness tracking for gender and ethnicity fields

## Overview

This project consists of three main components:

| Component | Description |
|-----------|-------------|
| **Extractor** | Python scripts that scrape data from SGDI and load it into BigQuery |
| **dbt** | Data transformation models that process raw data into analytical tables |
| **Docs** | Static site generator for the analytics dashboard |

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
├── dbt/                    # dbt transformation models
│   ├── models/
│   │   ├── staging/        # Raw data staging
│   │   ├── intermediate/   # Intermediate transformations
│   │   ├── dimensions/     # Dimension tables (dim_person, dim_departments)
│   │   └── facts/          # Fact tables (fact_current_roles, fact_role_history)
│   ├── seeds/              # Reference data (ethnicity patterns)
│   └── analyses/           # Ad-hoc SQL analyses
├── docs/                   # Static site generator
│   ├── generate.py         # Dashboard generator script
│   ├── templates/          # Jinja2 HTML templates
│   └── dist/               # Generated output (gitignored)
└── .github/workflows/      # CI/CD pipelines
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

## Quick Start (Recommended)

From a fresh clone, the fastest path is:

1. Put your service account key at [extractor/token/gcp_token.json](extractor/token/gcp_token.json)
2. Install dependencies:
       - Extractor: `cd extractor && uv sync`
       - dbt: `cd ../dbt && uv sync`
       - Docs: `cd ../docs && uv sync`
3. Validate setup:
       - `cd ../extractor`
       - `uv run python main.py --check`
4. Run extraction:
       - `uv run python main.py --ministry_extractor --organs_of_state_extractor`
5. Run transformations:
       - `cd ../dbt && uv run dbt run`
6. Build dashboard:
       - `cd ../docs && uv run python generate.py`

## Usage

### Pre-flight Checks

Before running any extraction, validate that all requirements are met:

```bash
cd extractor
uv run python main.py --check
```

Pre-flight checks currently validate:
- GCP token file format and required fields
- Required Python packages
- Network connectivity to SGDI (when extraction is requested)
- BigQuery connectivity and permissions (when a BigQuery-dependent job is requested)

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

### Process Slowly Changing Dimensions (SCD)

Convert raw data into SCD Type 2 format for historical tracking:

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
| `--verbose` | `-v` | Enable verbose/debug logging |

## Typical Workflows

### 1) Full refresh (most common)

```bash
cd extractor
uv run python main.py --ministry_extractor --organs_of_state_extractor
cd ../dbt
uv run dbt run
cd ../docs
uv run python generate.py
```

### 2) Resume after extractor interruption

```bash
cd extractor
uv run python main.py --ministry_extractor --ministry "Ministry of Health" --resume_run
```

### 3) Debug mode run

```bash
cd extractor
uv run python main.py --ministry_extractor --verbose
```

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

## dbt Models

To run dbt transformations:

```bash
cd dbt
uv sync
uv run dbt run
```

### Key Models

| Model | Description |
|-------|-------------|
| `dim_person` | Person dimension with inferred gender and ethnicity |
| `dim_departments` | Department hierarchy and metadata |
| `fact_current_roles` | Current role assignments |
| `fact_role_history` | Historical role changes (SCD Type 2) |
| `fact_people_changes` | Personnel movements and changes |

## Generating the Dashboard

The analytics dashboard is automatically deployed on every push to `main`. To generate locally:

```bash
cd docs
uv sync
uv run python generate.py
open dist/index.html
```

## Troubleshooting

- **Pre-flight fails on token checks**
       - Ensure [extractor/token/gcp_token.json](extractor/token/gcp_token.json) is a real service account key, not a placeholder.
- **BigQuery permission errors (403)**
       - Confirm service account has `BigQuery Data Editor` and `BigQuery Job User`.
- **Extractor fails due to network issues**
       - Re-run when SGDI is reachable, or isolate with a single ministry using `--ministry`.
- **Partial run and missing ministries/organs**
       - Use `--resume_run` together with `--ministry` or `--organs_of_state` to continue from a checkpoint.
- **Need to bypass checks temporarily**
       - Use `--skip_checks` only for controlled debugging, not normal execution.

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│    SGDI     │────▶│  Extractor  │────▶│  BigQuery   │
│  (Website)  │     │  (Python)   │     │   (Raw)     │
└─────────────┘     └─────────────┘     └──────┬──────┘
                                               │
                                               ▼
                                        ┌─────────────┐
                                        │     DBT     │
                                        │ (Transform) │
                                        └──────┬──────┘
                                               │
                                               ▼
                                        ┌─────────────┐     ┌─────────────┐
                                        │  BigQuery   │────▶│  Dashboard  │
                                        │ (Analytics) │     │   (HTML)    │
                                        └─────────────┘     └─────────────┘
```

## License

This project is for educational and research purposes. Data sourced from [SGDI](https://www.sgdi.gov.sg/) is subject to Singapore Government Terms of Use.
