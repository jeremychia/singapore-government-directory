import numpy as np
import pandas as pd
from gbq import PROJECT_ID, save_to_bigquery
from logger import get_logger, LogContext
from slowly_changing_dimensions.download_sources import DownloadSources

logger = get_logger(__name__)


class ConvertToSCD:
    def __init__(self):
        self.download_sources = DownloadSources()
        self.project_id = PROJECT_ID
        self.target_schema = "scd"
        logger.debug("Initialized ConvertToSCD processor")

    def process_and_upload(self):
        """Processes names and departments, then uploads to BigQuery."""
        
        with LogContext(logger, "Downloading source data"):
            (
                concat_names,
                last_download__names,
                concat_departments,
                last_download__departments,
            ) = self.download_sources.run()
        
        logger.info(f"Downloaded {len(concat_names)} name records and {len(concat_departments)} department records")

        # Process names
        with LogContext(logger, "Processing names SCD"):
            name_processor = NameProcessor(concat_names, last_download__names)
            new_scd_names = name_processor.process_concat_names()
            logger.info(f"Processed {len(new_scd_names)} SCD name records")
            save_to_bigquery(new_scd_names, self.project_id, self.target_schema, "names")

        # Process departments
        with LogContext(logger, "Processing departments SCD"):
            department_processor = DepartmentProcessor(
                concat_departments, last_download__departments
            )
            new_scd_departments = department_processor.process_concat_departments()
            logger.info(f"Processed {len(new_scd_departments)} SCD department records")
            save_to_bigquery(
                new_scd_departments, self.project_id, self.target_schema, "departments"
            )


class NameProcessor:
    def __init__(self, concat_names, last_download__names):
        self.concat_names = concat_names
        self.last_download__names = last_download__names
        self.grouping_cols = [
            "name_uuid",
            "position",
            "name",
            "email",
            "department",
            "url",
            "ministry_name",
        ]

    def process_name_uuid_group(self, group):
        """Processes a group of rows with the same name_uuid."""

        len_group = len(group)

        if len_group == 1:
            return group

        if len_group == 2:
            return self.handle_two_rows(group)

        # len_group > 2
        return self.handle_multiple_rows(group)

    def handle_two_rows(self, group):
        """Handles groups with exactly two rows."""

        ministry_name = group.iloc[0]["ministry_name"]
        last_downloaded = self.get_latest_accessed(ministry_name)

        if group.iloc[0]["_valid_to"] == last_downloaded:
            return self.merge_rows(group)
        return group

    def handle_multiple_rows(self, group):
        """Handles groups with more than two rows."""

        result = [group.iloc[:-2]]
        remaining_group = group.iloc[-2:].reset_index(drop=True)
        result.append(self.handle_two_rows(remaining_group))
        return pd.concat(result)

    def get_latest_accessed(self, ministry_name):
        """Retrieves the latest_accessed date for a given ministry."""

        ministry_filter = self.last_download__names["ministry_name"] == ministry_name
        return self.last_download__names.loc[ministry_filter, "latest_accessed"].iloc[0]

    def merge_rows(self, group):
        """Merges rows based on specified columns and aggregates _valid_from and _valid_to."""
        
        # When we're in a grouped context (grouped by name_uuid), name_uuid is not available as a column
        # So we need to use the grouping columns excluding name_uuid
        available_grouping_cols = [col for col in self.grouping_cols if col in group.columns]
        
        if not available_grouping_cols:
            # If no grouping columns are available, return the group as-is
            return group

        return group.groupby(available_grouping_cols, as_index=False).agg(
            _valid_from=("_valid_from", "min"), _valid_to=("_valid_to", "max")
        )

    def process_concat_names(self):
        """Processes the concat_names DataFrame and returns the new_scd_names DataFrame."""

        grouped = self.concat_names.groupby("name_uuid", group_keys=False)
        result_dfs = grouped.apply(self.process_name_uuid_group)
        return result_dfs.reset_index(drop=True)


class DepartmentProcessor:
    def __init__(self, concat_departments, last_download__departments):
        self.concat_departments = concat_departments
        self.last_download__departments = last_download__departments
        self.departments_groupby_columns = [
            "department_uuid",
            "parent_name",
            "department_name",
            "department_link",
            "children",
            "ministry_name",
        ]

    def process_department_uuid_group(self, group):
        """Processes a group of rows with the same department_uuid."""

        len_group = len(group)

        if len_group == 1:
            return group

        if len_group == 2:
            return self.handle_two_department_rows(group)

        # len_group > 2
        return self.handle_multiple_department_rows(group)

    def handle_two_department_rows(self, group):
        """Handles groups with exactly two rows for departments."""

        ministry_name = group.iloc[0]["ministry_name"]
        last_downloaded = self.get_latest_accessed_department(ministry_name)

        if group.iloc[0]["_valid_to"] == last_downloaded:
            return self.merge_department_rows(group)
        return group

    def handle_multiple_department_rows(self, group):
        """Handles groups with more than two rows for departments."""

        result = [group.iloc[:-2]]
        remaining_group = group.iloc[-2:].reset_index(drop=True)
        result.append(self.handle_two_department_rows(remaining_group))
        return pd.concat(result)

    def get_latest_accessed_department(self, ministry_name):
        """Retrieves the latest_accessed date for a given ministry for departments."""

        ministry_filter = (
            self.last_download__departments["ministry_name"] == ministry_name
        )
        return self.last_download__departments.loc[
            ministry_filter, "latest_accessed"
        ].iloc[0]

    def merge_department_rows(self, group):
        """Merges rows based on specified columns, aggregates, and restores nulls in parent_name."""

        # Fill null parent_name with a placeholder
        filled_group = group.fillna({"parent_name": "Unknown Parent"})

        # Perform groupby and aggregation
        result = filled_group.groupby(
            self.departments_groupby_columns, as_index=False
        ).agg(_valid_from=("_valid_from", "min"), _valid_to=("_valid_to", "max"))

        # Restore nulls in parent_name
        result["parent_name"] = result["parent_name"].replace("Unknown Parent", np.nan)

        return result

    def process_concat_departments(self):
        """Processes the concat_departments DataFrame and returns the new_scd_departments DataFrame."""

        departments_dfs = []
        for department_uuid, group in self.concat_departments.groupby(
            "department_uuid"
        ):
            departments_dfs.append(self.process_department_uuid_group(group))

        return pd.concat(departments_dfs, ignore_index=True)
