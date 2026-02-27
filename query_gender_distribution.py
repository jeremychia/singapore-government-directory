#!/usr/bin/env python3
"""Query gender distribution from dim_person"""

import json
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account

# Load credentials
creds_path = Path("dbt/token/gcp_token.json")
credentials = service_account.Credentials.from_service_account_file(str(creds_path))
client = bigquery.Client(project="singapore-government-directory", credentials=credentials)

query = """
SELECT 
    predicted_gender,
    COUNT(*) as count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct
FROM `singapore-government-directory.dbt.dim_person`
GROUP BY predicted_gender
ORDER BY count DESC
"""

print("\n" + "="*60)
print("Gender Distribution in dim_person (after improvements)")
print("="*60)

results = client.query(query).result()
for row in results:
    gender = row.predicted_gender if row.predicted_gender else "NULL"
    print(f"{gender:15} | {row.count:10,} | {row.pct:6.2f}%")

print("="*60 + "\n")
