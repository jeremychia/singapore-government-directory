import pandas as pd
import utils
from gbq import PROJECT_ID, append_in_bigquery
from logger import get_logger, LogContext
from ministries.ministry_explorer import MinistryExplorer

logger = get_logger(__name__)


class MinistryDataProcessor:
    def __init__(self, ministry_name, url):
        self.ministry_name = ministry_name
        self.url = url
        self.schema = "raw"
        self.project_id = PROJECT_ID
        self.explorer = MinistryExplorer(ministry_name, url)
        logger.debug(f"Initialized MinistryDataProcessor for {ministry_name}")

    def explore_ministries(self):
        """Explore the ministries and return names and departments."""
        logger.debug(f"Exploring ministries for {self.ministry_name}")
        return self.explorer.explore_ministries()

    def process_names(self, names):
        """Process the names data: add ministry and timestamp."""
        logger.debug(f"Processing {len(names)} names")
        names_df = pd.DataFrame(names)
        names_df = self._add_ministry_and_timestamp(names_df)
        return names_df

    def process_departments(self, departments):
        """Process the departments data: unwrap tree structure, add ministry, and timestamp."""
        rows = self._unwrap_departments(departments)
        logger.debug(f"Processing {len(rows)} department rows")
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
        self, names_df, departments_df, names_datetime, departments_datetime,
        exploration_duration_seconds=None
    ):
        """Create metadata DataFrame."""
        table_name = ["names", "departments"]
        ministry_name = [self.ministry_name] * len(table_name)
        num_rows = [len(names_df), len(departments_df)]
        accessed_at = [names_datetime, departments_datetime]
        duration_seconds = [exploration_duration_seconds, exploration_duration_seconds]

        metadata_df = pd.DataFrame(
            {
                "table_name": table_name,
                "ministry_name": ministry_name,
                "num_rows": num_rows,
                "_accessed_at": accessed_at,
                "extraction_duration_seconds": duration_seconds,
            }
        )
        logger.debug(f"Created metadata: names={num_rows[0]}, departments={num_rows[1]}, duration={exploration_duration_seconds}s")
        return metadata_df

    def process_and_upload(self):
        """Main function to process and upload names, departments, and metadata."""
        with LogContext(logger, "Exploring ministry structure", ministry=self.ministry_name):
            names, departments = self.explore_ministries()
        
        # Get exploration duration from the explorer
        exploration_duration = self.explorer.get_exploration_duration()
        
        logger.info(f"Found {len(names)} names and {len(departments)} departments")

        with LogContext(logger, "Processing data"):
            names_df, names_datetime = self.process_names(names)
            departments_df, departments_datetime = self.process_departments(departments)
            metadata_df = self.create_metadata(
                names_df, departments_df, names_datetime, departments_datetime,
                exploration_duration_seconds=exploration_duration
            )

        with LogContext(logger, "Uploading to BigQuery"):
            logger.info(f"Uploading {len(names_df)} names to {self.schema}.names")
            append_in_bigquery(names_df, self.project_id, self.schema, "names")
            
            logger.info(f"Uploading {len(departments_df)} departments to {self.schema}.departments")
            append_in_bigquery(departments_df, self.project_id, self.schema, "departments")
            
            logger.info("Uploading metadata")
            append_in_bigquery(metadata_df, self.project_id, self.schema, "metadata")
        
        logger.info(f"Successfully processed {self.ministry_name}")
