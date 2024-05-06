import pandas as pd
import pandas_gbq
import os

import utils
import ministries
from ministries.ministry_explorer import MinistryExplorer

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "token/gcp_token.json"

ministries_url = ministries.ministries_url

project_id = "singapore-parliament-speeches"
schema = "sg_govt_dir"

for ministry_name, url in ministries_url.items():
    explorer = MinistryExplorer(ministry_name, url)
    names, departments = explorer.explore_ministries()

    names_df = pd.DataFrame(names)
    names_df = utils.add_ministry(names_df, ministry_name)
    names_df = utils.add_timestamp(names_df)

    pandas_gbq.to_gbq(
        names_df,
        destination_table=f"{schema}.names",
        project_id=project_id,
        if_exists="append",
    )

    rows = []
    for department in departments:
        rows.extend(utils.unwrap_tree(department))

    departments_df = pd.DataFrame(rows)
    departments_df = utils.add_ministry(departments_df, ministry_name)
    departments_df = utils.add_timestamp(departments_df)

    pandas_gbq.to_gbq(
        departments_df,
        destination_table=f"{schema}.departments",
        project_id=project_id,
        if_exists="append",
    )
