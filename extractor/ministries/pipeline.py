import pandas as pd
import utils
from gbq import PROJECT_ID, upload_to_bigquery
from ministries.ministry_explorer import MinistryExplorer


class MinistryDataProcessor:
    def __init__(self, ministry_name, url):
        self.ministry_name = ministry_name
        self.url = url
        self.schema = "raw"
        self.project_id = PROJECT_ID
        self.explorer = MinistryExplorer(ministry_name, url)

    def explore_ministries(self):
        """Explore the ministries and return names and departments."""
        return self.explorer.explore_ministries()

    def process_names(self, names):
        """Process the names data: add ministry and timestamp."""
        names_df = pd.DataFrame(names)
        names_df = self._add_ministry_and_timestamp(names_df)
        return names_df

    def process_departments(self, departments):
        """Process the departments data: unwrap tree structure, add ministry, and timestamp."""
        rows = self._unwrap_departments(departments)
        departments_df = pd.DataFrame(rows)
        departments_df = self._add_ministry_and_timestamp(departments_df)
        return departments_df

    def _unwrap_departments(self, departments):
        """Unwrap the tree structure of departments."""
        rows = []
        for department in departments:
            rows.extend(utils.unwrap_tree(department))
        return rows

    def _add_ministry_and_timestamp(self, df):
        """Add ministry and timestamp to a DataFrame."""
        df = utils.add_ministry(df, self.ministry_name)
        df, timestamp = utils.add_timestamp(df)
        return df, timestamp

    def create_metadata(
        self, names_df, departments_df, names_datetime, departments_datetime
    ):
        """Create metadata DataFrame."""
        table_name = ["names", "departments"]
        ministry_name = [self.ministry_name] * len(table_name)
        num_rows = [len(names_df), len(departments_df)]
        accessed_at = [names_datetime, departments_datetime]

        metadata_df = pd.DataFrame(
            {
                "table_name": table_name,
                "ministry_name": ministry_name,
                "num_rows": num_rows,
                "_accessed_at": accessed_at,
            }
        )
        return metadata_df

    def process_and_upload(self):
        """Main function to process and upload names, departments, and metadata."""
        names, departments = self.explore_ministries()

        names_df, names_datetime = self.process_names(names)
        departments_df, departments_datetime = self.process_departments(departments)
        metadata_df = self.create_metadata(
            names_df, departments_df, names_datetime, departments_datetime
        )

        upload_to_bigquery(names_df, "names")
        upload_to_bigquery(departments_df, "departments")
        upload_to_bigquery(metadata_df, "metadata")
