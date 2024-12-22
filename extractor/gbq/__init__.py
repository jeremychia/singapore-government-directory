import pandas_gbq

PROJECT_ID = "singapore-government-directory"


def append_in_bigquery(df, project_id, schema, table_name):
    """Upload a DataFrame to BigQuery."""
    pandas_gbq.to_gbq(
        df,
        destination_table=f"{schema}.{table_name}",
        project_id=project_id,
        if_exists="append",
    )


def save_to_bigquery(df, project_id, schema, table_name):
    pandas_gbq.to_gbq(
        df,
        destination_table=f"{schema}.{table_name}",
        project_id=project_id,
        if_exists="replace",
    )
