import pandas as pd
import pandas_gbq
from gbq import PROJECT_ID


class DownloadSources:
    def __init__(self):
        self.project_id = PROJECT_ID

        self.source_schema = "raw"
        self.target_schema = "scd"

    def download_names(self):
        names_sql = f"""
        select
            md5(
                concat(
                    coalesce(position, ""),
                    coalesce(name, ""),
                    coalesce(email, ""),
                    coalesce(department, ""),
                    coalesce(url, ""),
                    coalesce(ministry_name, "")
                )
            ) as name_uuid,
            position,
            name,
            email,
            department,
            url,
            ministry_name,
            _accessed_at as _valid_from,
            _accessed_at as _valid_to,
        from `{self.project_id}.{self.source_schema}.names`
        where date(_accessed_at) = (select max(date(_accessed_at)) from `{self.project_id}.{self.source_schema}.names`)
        group by all
        """

        current_names_df = pandas_gbq.read_gbq(
            query_or_table=names_sql, project_id=self.project_id
        )

        scd_names_df = pandas_gbq.read_gbq(
            query_or_table=f"{self.target_schema}.names", project_id=self.project_id
        )

        concat_names = pd.concat([scd_names_df, current_names_df]).sort_values(
            by=["name_uuid", "_valid_from"], ascending=True
        )

        last_download__names = scd_names_df.groupby(
            "ministry_name", as_index=False
        ).agg(latest_accessed=("_valid_to", "max"))

        return concat_names, last_download__names

    def download_departments(self):
        departments_sql = f"""
        select
            md5(
                concat(
                    coalesce(parent_name, ""),
                    coalesce(department_name, ""),
                    coalesce(department_link, ""),
                    coalesce(children, ""),
                    coalesce(ministry_name, "")
                )
            ) as department_uuid,
            parent_name,
            department_name,
            department_link,
            children,
            ministry_name,
            _accessed_at as _valid_from,
            _accessed_at as _valid_to,
        from `{self.project_id}.{self.source_schema}.departments`
        where date(_accessed_at) = (select max(date(_accessed_at)) from `{self.project_id}.{self.source_schema}.departments`)
        group by all
        """

        current_departments_df = pandas_gbq.read_gbq(
            query_or_table=departments_sql, project_id=self.project_id
        )

        scd_departments_df = pandas_gbq.read_gbq(
            query_or_table=f"{self.target_schema}.departments",
            project_id=self.project_id,
        )

        concat_departments = pd.concat(
            [scd_departments_df, current_departments_df]
        ).sort_values(by=["department_uuid", "_valid_from"], ascending=True)

        last_download__departments = scd_departments_df.groupby(
            "ministry_name", as_index=False
        ).agg(latest_accessed=("_valid_to", "max"))

        return concat_departments, last_download__departments

    def run(self):
        concat_names, last_download__names = self.download_names()
        (
            concat_departments,
            last_download__departments,
        ) = self.download_departments()

        return (
            concat_names,
            last_download__names,
            concat_departments,
            last_download__departments,
        )
