import pandas as pd
import pandas_gbq
import numpy as np
import os

import utils
from utils.names import NameProcessor
import ministries
from ministries.ministry_explorer import MinistryExplorer

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "token/gcp_token.json"

ministries_url = ministries.ministries_url

project_id = "singapore-parliament-speeches"
schema = "sg_govt_dir"

# download data

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

# process names

project_id = "singapore-parliament-speeches"
schema = "sg_govt_dir_preprocess"

query = """
select *
from `sg_govt_dir.names`
"""

names_all = pandas_gbq.read_gbq(query, project_id)
NameProcessor.process_names(names_all)

print("Processing: Names Mapping")
names_mapping = (
    names_all[["extracted_name", "name"]]
    .drop_duplicates()
    .reset_index(inplace=False)
    .drop("index", axis=1)
)

pandas_gbq.to_gbq(
    names_mapping,
    destination_table=f"{schema}.names_mapping",
    project_id=project_id,
    if_exists="replace",
)

print("Processing: Postfixes History")
postfixes_history = (
    names_all.groupby(["extracted_name", "postfix"])
    .agg(effective_from=("_accessed_at", np.min), effective_to=("_accessed_at", np.max))
    .reset_index()
)

pandas_gbq.to_gbq(
    postfixes_history,
    destination_table=f"{schema}.postfixes_history",
    project_id=project_id,
    if_exists="replace",
)

print("Processing: Prefixes History")
prefixes_history = (
    names_all.groupby(["extracted_name", "prefix"])
    .agg(effective_from=("_accessed_at", np.min), effective_to=("_accessed_at", np.max))
    .reset_index()
)

pandas_gbq.to_gbq(
    prefixes_history,
    destination_table=f"{schema}.prefixes_history",
    project_id=project_id,
    if_exists="replace",
)
