import pandas_gbq
from logger import get_logger

logger = get_logger(__name__)

PROJECT_ID = "singapore-government-directory"


def append_in_bigquery(df, project_id, schema, table_name):
    """Upload a DataFrame to BigQuery by appending rows."""
    destination = f"{schema}.{table_name}"
    logger.debug(f"Appending {len(df)} rows to {destination}")
    try:
        pandas_gbq.to_gbq(
            df,
            destination_table=destination,
            project_id=project_id,
            if_exists="append",
        )
        logger.debug(f"Successfully appended to {destination}")
    except Exception as e:
        logger.error(f"Failed to append to {destination}: {e}")
        raise


def save_to_bigquery(df, project_id, schema, table_name):
    """Upload a DataFrame to BigQuery by replacing the table."""
    destination = f"{schema}.{table_name}"
    logger.debug(f"Replacing {destination} with {len(df)} rows")
    try:
        pandas_gbq.to_gbq(
            df,
            destination_table=destination,
            project_id=project_id,
            if_exists="replace",
        )
        logger.debug(f"Successfully replaced {destination}")
    except Exception as e:
        logger.error(f"Failed to replace {destination}: {e}")
        raise
