import numpy as np
import pandas as pd
import pandas_gbq
from gbq import PROJECT_ID, save_to_bigquery
from name_cleaning.name_processor import NameProcessor


class NameProcessorPipeline:
    def __init__(self):
        self.project_id = PROJECT_ID
        self.schema = "preprocessed"

    # Function to execute the query and retrieve data
    def fetch_data(self, query):
        return pandas_gbq.read_gbq(query, self.project_id)

    # Function to process names
    def process_names(self, names_all):
        NameProcessor.process_names(names_all)

    # Function to create a names mapping
    def create_names_mapping(self, names_all):
        return (
            names_all[["extracted_name", "name"]]
            .drop_duplicates()
            .reset_index(inplace=False)
            .drop("index", axis=1)
        )

    # Function to create history (postfixes or prefixes)
    def create_history(self, names_all, group_by_columns):
        return (
            names_all.groupby(group_by_columns)
            .agg(
                effective_from=("_accessed_at", np.min),
                effective_to=("_accessed_at", np.max),
            )
            .reset_index()
        )

    # Main function to orchestrate the pipeline
    def run(self):
        # only extract minimum and maximum values, as in-between values do not matter
        query = """
        select name, min(_accessed_at) _accessed_at
        from `raw.names`
        group by all
        union all
        select name, max(_accessed_at) as _accessed_at
        from `raw.names`
        group by all
        """

        # Step 1: Fetch data
        names_all = self.fetch_data(query)

        # Step 2: Process names
        self.process_names(names_all)

        # Step 3: Create and save names mapping
        print("Processing: Names Mapping")
        names_mapping = self.create_names_mapping(names_all)
        save_to_bigquery(names_mapping, self.project_id, self.schema, "names_mapping")

        # Step 4: Create and save postfixes history
        print("Processing: Postfixes History")
        postfixes_history = self.create_history(
            names_all, ["extracted_name", "postfix"]
        )
        save_to_bigquery(
            postfixes_history, self.project_id, self.schema, "postfixes_history"
        )

        # Step 5: Create and save prefixes history
        print("Processing: Prefixes History")
        prefixes_history = self.create_history(names_all, ["extracted_name", "prefix"])
        save_to_bigquery(
            prefixes_history, self.project_id, self.schema, "prefixes_history"
        )
