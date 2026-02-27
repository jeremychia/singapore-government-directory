import numpy as np
import pandas as pd
import pandas_gbq
from gbq import PROJECT_ID, save_to_bigquery
from logger import get_logger, LogContext
from name_cleaning.name_processor import NameProcessor

logger = get_logger(__name__)


class NameProcessorPipeline:
    def __init__(self):
        self.project_id = PROJECT_ID
        self.schema = "preprocessed"
        logger.debug("Initialized NameProcessorPipeline")

    # Function to execute the query and retrieve data
    def fetch_data(self, query):
        logger.debug("Fetching data from BigQuery")
        df = pandas_gbq.read_gbq(query, self.project_id)
        logger.debug(f"Fetched {len(df)} records")
        return df

    # Function to process names
    def process_names(self, names_all):
        logger.debug(f"Processing {len(names_all)} names")
        NameProcessor.process_names(names_all)

    # Function to create a names mapping
    def create_names_mapping(self, names_all):
        mapping = (
            names_all[["extracted_name", "name", "email"]]
            .drop_duplicates()
            .reset_index(inplace=False)
            .drop("index", axis=1)
        )
        logger.debug(f"Created names mapping with {len(mapping)} entries")
        return mapping

    # Function to create history (postfixes or prefixes)
    def create_history(self, names_all, group_by_columns):
        history = (
            names_all.groupby(group_by_columns)
            .agg(
                effective_from=("_valid_from", np.min),
                effective_to=("_valid_to", np.max),
            )
            .reset_index()
        )
        logger.debug(f"Created history with {len(history)} entries")
        return history

    # Main function to orchestrate the pipeline
    def run(self):
        # only extract minimum and maximum values, as in-between values do not matter
        query = """
        select name, lower(email) as email, _valid_from, _valid_to
        from `singapore-government-directory.scd.names`
        """

        # Step 1: Fetch data
        with LogContext(logger, "Fetching name data"):
            names_all = self.fetch_data(query)

        # Step 2: Process names
        with LogContext(logger, "Processing names"):
            self.process_names(names_all)

        # Step 3: Create and save names mapping
        with LogContext(logger, "Creating names mapping"):
            names_mapping = self.create_names_mapping(names_all)
            save_to_bigquery(names_mapping, self.project_id, self.schema, "names_mapping")
            logger.info(f"Saved {len(names_mapping)} name mappings")

        # Step 4: Create and save postfixes history
        with LogContext(logger, "Creating postfixes history"):
            postfixes_history = self.create_history(
                names_all, ["email", "extracted_name", "postfix"]
            )
            save_to_bigquery(
                postfixes_history, self.project_id, self.schema, "postfixes_history"
            )
            logger.info(f"Saved {len(postfixes_history)} postfix records")

        # Step 5: Create and save prefixes history
        with LogContext(logger, "Creating prefixes history"):
            prefixes_history = self.create_history(
                names_all, ["email", "extracted_name", "prefix"]
            )
            save_to_bigquery(
                prefixes_history, self.project_id, self.schema, "prefixes_history"
            )
            logger.info(f"Saved {len(prefixes_history)} prefix records")
