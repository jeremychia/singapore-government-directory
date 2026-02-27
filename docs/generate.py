#!/usr/bin/env python3
"""
Generate static HTML documentation for Singapore Government Directory analytics.

Usage:
    uv run python generate.py
    
Output:
    dist/index.html - Static HTML page with visualizations
"""

import json
from pathlib import Path
from datetime import datetime

from google.cloud import bigquery
from google.oauth2 import service_account
from jinja2 import Environment, FileSystemLoader

# Configuration
PROJECT_ID = "singapore-government-directory"
DATASET_DIMENSIONS = "dbt_dimensions"
DATASET_FACTS = "dbt_facts"
LOCATION = "EU"
CREDS_PATH = Path(__file__).parent.parent / "extractor" / "token" / "gcp_token.json"
OUTPUT_DIR = Path(__file__).parent / "dist"
TEMPLATES_DIR = Path(__file__).parent / "templates"

# Organs of State (not Ministries) - must match exact names in data
ORGANS_OF_STATE = [
    "ATTORNEY-GENERAL'S CHAMBERS",
    "AUDITOR-GENERAL'S OFFICE",
    "INDUSTRIAL ARBITRATION COURT",
    "ISTANA",
    "JUDICIARY, FAMILY JUSTICE COURTS",
    "JUDICIARY, STATE COURTS",
    "JUDICIARY, SUPREME COURT",
    "PARLIAMENT OF SINGAPORE",
    "PUBLIC SERVICE COMMISSION",
    "THE CABINET",
]

# Ministry name mappings for renamed/reorganised ministries
# Maps old names to current names so they are grouped together
MINISTRY_NAME_MAPPING = {
    "Ministry of Communications and Information": "Ministry of Digital Development and Information",
}


def normalize_ministry_name(name: str) -> str:
    """Normalize ministry name using mapping."""
    return MINISTRY_NAME_MAPPING.get(name, name)


def get_bigquery_client() -> bigquery.Client:
    """Create BigQuery client using GCP token."""
    credentials = service_account.Credentials.from_service_account_file(str(CREDS_PATH))
    return bigquery.Client(project=PROJECT_ID, credentials=credentials, location=LOCATION)


def query_to_dict(client: bigquery.Client, query: str) -> list[dict]:
    """Execute query and return results as list of dicts."""
    results = client.query(query).result()
    return [dict(row) for row in results]


def get_gender_distribution(client: bigquery.Client) -> list[dict]:
    """Get overall gender distribution."""
    query = f"""
    SELECT 
        COALESCE(predicted_gender, 'Unknown') as gender,
        COUNT(*) as count
    FROM `{PROJECT_ID}.{DATASET_DIMENSIONS}.dim_person`
    GROUP BY predicted_gender
    ORDER BY count DESC
    """
    return query_to_dict(client, query)


def get_ethnicity_distribution(client: bigquery.Client) -> list[dict]:
    """Get overall ethnicity distribution."""
    query = f"""
    SELECT 
        COALESCE(predicted_ethnicity, 'Unknown') as ethnicity,
        COUNT(*) as count
    FROM `{PROJECT_ID}.{DATASET_DIMENSIONS}.dim_person`
    GROUP BY predicted_ethnicity
    ORDER BY count DESC
    """
    return query_to_dict(client, query)


def get_gender_by_ministry(client: bigquery.Client) -> list[dict]:
    """Get gender distribution by ministry."""
    query = f"""
    SELECT 
        f.ministry_name,
        COALESCE(p.predicted_gender, 'Unknown') as gender,
        COUNT(DISTINCT f.person_key) as count
    FROM `{PROJECT_ID}.{DATASET_FACTS}.fact_current_roles` f
    JOIN `{PROJECT_ID}.{DATASET_DIMENSIONS}.dim_person` p ON f.person_key = p.person_key
    WHERE f.ministry_name IS NOT NULL
    GROUP BY f.ministry_name, p.predicted_gender
    ORDER BY f.ministry_name, count DESC
    """
    return query_to_dict(client, query)


def get_ethnicity_by_ministry(client: bigquery.Client) -> list[dict]:
    """Get ethnicity distribution by ministry."""
    query = f"""
    SELECT 
        f.ministry_name,
        COALESCE(p.predicted_ethnicity, 'Unknown') as ethnicity,
        COUNT(DISTINCT f.person_key) as count
    FROM `{PROJECT_ID}.{DATASET_FACTS}.fact_current_roles` f
    JOIN `{PROJECT_ID}.{DATASET_DIMENSIONS}.dim_person` p ON f.person_key = p.person_key
    WHERE f.ministry_name IS NOT NULL
    GROUP BY f.ministry_name, p.predicted_ethnicity
    ORDER BY f.ministry_name, count DESC
    """
    return query_to_dict(client, query)


def get_data_quality_metrics(client: bigquery.Client) -> dict:
    """Get data quality/completeness metrics."""
    query = f"""
    SELECT 
        COUNT(*) as total_persons,
        COUNTIF(predicted_gender IS NOT NULL AND predicted_gender != 'Unknown') as has_gender,
        COUNTIF(predicted_ethnicity IS NOT NULL AND predicted_ethnicity != 'Unknown') as has_ethnicity,
        COUNTIF(email IS NOT NULL) as has_email,
        COUNTIF(has_personal_email = TRUE) as has_personal_email,
        COUNTIF(prefix IS NOT NULL) as has_prefix,
        COUNTIF(postfix IS NOT NULL) as has_postfix
    FROM `{PROJECT_ID}.{DATASET_DIMENSIONS}.dim_person`
    """
    results = query_to_dict(client, query)
    if results:
        row = results[0]
        total = row["total_persons"]
        return {
            "total_persons": total,
            "metrics": [
                {
                    "name": "Gender Predicted",
                    "count": row["has_gender"],
                    "pct": round(100 * row["has_gender"] / total, 1) if total else 0,
                },
                {
                    "name": "Ethnicity Predicted",
                    "count": row["has_ethnicity"],
                    "pct": round(100 * row["has_ethnicity"] / total, 1) if total else 0,
                },
                {
                    "name": "Has Email",
                    "count": row["has_email"],
                    "pct": round(100 * row["has_email"] / total, 1) if total else 0,
                },
                {
                    "name": "Has Personal Email",
                    "count": row["has_personal_email"],
                    "pct": round(100 * row["has_personal_email"] / total, 1) if total else 0,
                },
                {
                    "name": "Has Prefix (Dr, Prof, etc.)",
                    "count": row["has_prefix"],
                    "pct": round(100 * row["has_prefix"] / total, 1) if total else 0,
                },
                {
                    "name": "Has Postfix (PhD, PBM, etc.)",
                    "count": row["has_postfix"],
                    "pct": round(100 * row["has_postfix"] / total, 1) if total else 0,
                },
            ],
        }
    return {"total_persons": 0, "metrics": []}


def get_ministry_headcount(client: bigquery.Client) -> list[dict]:
    """Get headcount per ministry."""
    query = f"""
    SELECT 
        ministry_name,
        COUNT(DISTINCT person_key) as headcount
    FROM `{PROJECT_ID}.{DATASET_FACTS}.fact_current_roles`
    WHERE ministry_name IS NOT NULL
    GROUP BY ministry_name
    ORDER BY headcount DESC
    """
    return query_to_dict(client, query)


def normalize_and_aggregate(data: list[dict], category_field: str) -> list[dict]:
    """
    Normalize ministry names and aggregate counts for renamed ministries.
    """
    from collections import defaultdict
    
    aggregated = defaultdict(lambda: defaultdict(int))
    
    for row in data:
        normalized_name = normalize_ministry_name(row["ministry_name"])
        category = row[category_field]
        aggregated[normalized_name][category] += row["count"]
    
    result = []
    for ministry_name, categories in aggregated.items():
        for category, count in categories.items():
            result.append({
                "ministry_name": ministry_name,
                category_field: category,
                "count": count
            })
    
    return result


def normalize_headcount(data: list[dict]) -> list[dict]:
    """
    Normalize ministry names and aggregate headcounts for renamed ministries.
    """
    from collections import defaultdict
    
    aggregated = defaultdict(int)
    
    for row in data:
        normalized_name = normalize_ministry_name(row["ministry_name"])
        aggregated[normalized_name] += row["headcount"]
    
    result = [{"ministry_name": name, "headcount": count} for name, count in aggregated.items()]
    return sorted(result, key=lambda x: x["headcount"], reverse=True)


def pivot_by_ministry(data: list[dict], category_field: str) -> dict:
    """
    Pivot data by ministry for stacked charts.
    Returns: {ministries: [...], categories: [...], data: {category: [counts...]}}
    """
    # Get unique ministries and categories
    ministries = sorted(set(row["ministry_name"] for row in data))
    categories = sorted(set(row[category_field] for row in data))
    
    # Build lookup
    lookup = {}
    for row in data:
        key = (row["ministry_name"], row[category_field])
        lookup[key] = row["count"]
    
    # Build data structure for Chart.js
    chart_data = {}
    for category in categories:
        chart_data[category] = [lookup.get((m, category), 0) for m in ministries]
    
    return {
        "ministries": ministries,
        "categories": categories,
        "data": chart_data,
    }


def separate_ministries_and_organs(data: list[dict], category_field: str) -> tuple[dict, dict]:
    """
    Separate data into ministries and organs of state, then pivot each.
    """
    ministries_data = [row for row in data if row["ministry_name"] not in ORGANS_OF_STATE]
    organs_data = [row for row in data if row["ministry_name"] in ORGANS_OF_STATE]
    
    ministries_pivot = pivot_by_ministry(ministries_data, category_field) if ministries_data else {"ministries": [], "categories": [], "data": {}}
    organs_pivot = pivot_by_ministry(organs_data, category_field) if organs_data else {"ministries": [], "categories": [], "data": {}}
    
    return ministries_pivot, organs_pivot


def separate_headcount(data: list[dict]) -> tuple[list[dict], list[dict]]:
    """Separate headcount into ministries and organs of state."""
    ministries = [row for row in data if row["ministry_name"] not in ORGANS_OF_STATE]
    organs = [row for row in data if row["ministry_name"] in ORGANS_OF_STATE]
    return ministries, organs


def main():
    print("Generating static documentation...")
    
    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Connect to BigQuery
    print(f"Connecting to BigQuery using {CREDS_PATH}...")
    client = get_bigquery_client()
    
    # Fetch data
    print("Fetching data...")
    gender_dist = get_gender_distribution(client)
    ethnicity_dist = get_ethnicity_distribution(client)
    gender_by_ministry_raw = get_gender_by_ministry(client)
    ethnicity_by_ministry_raw = get_ethnicity_by_ministry(client)
    data_quality = get_data_quality_metrics(client)
    ministry_headcount_raw = get_ministry_headcount(client)
    
    # Normalize ministry names (consolidate renamed ministries)
    gender_by_ministry = normalize_and_aggregate(gender_by_ministry_raw, "gender")
    ethnicity_by_ministry = normalize_and_aggregate(ethnicity_by_ministry_raw, "ethnicity")
    ministry_headcount = normalize_headcount(ministry_headcount_raw)
    
    # Separate ministries and organs of state
    gender_ministries, gender_organs = separate_ministries_and_organs(gender_by_ministry, "gender")
    ethnicity_ministries, ethnicity_organs = separate_ministries_and_organs(ethnicity_by_ministry, "ethnicity")
    headcount_ministries, headcount_organs = separate_headcount(ministry_headcount)
    
    # Prepare template context
    context = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "gender_distribution": gender_dist,
        "ethnicity_distribution": ethnicity_dist,
        "gender_by_ministry": json.dumps(gender_ministries),
        "gender_by_organs": json.dumps(gender_organs),
        "ethnicity_by_ministry": json.dumps(ethnicity_ministries),
        "ethnicity_by_organs": json.dumps(ethnicity_organs),
        "data_quality": data_quality,
        "ministry_headcount": headcount_ministries,
        "organs_headcount": headcount_organs,
    }
    
    # Render template
    print("Rendering HTML...")
    env = Environment(loader=FileSystemLoader(TEMPLATES_DIR))
    template = env.get_template("index.html")
    html = template.render(**context)
    
    # Write output
    output_file = OUTPUT_DIR / "index.html"
    output_file.write_text(html)
    print(f"Generated: {output_file}")
    
    print("Done!")


if __name__ == "__main__":
    main()
