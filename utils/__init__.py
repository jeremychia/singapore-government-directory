from datetime import datetime


def unwrap_tree(department, parent_name=None):
    rows = []
    department_name = department["name"]
    department_link = department["link"]
    children = department.get("children", [])

    department_info = {
        "parent_name": parent_name,
        "department_name": department_name,
        "department_link": department_link,
        "children": ", ".join([child["name"] for child in children]),
    }
    rows.append(department_info)

    for child in children:
        rows.extend(unwrap_tree(child, department_name))

    return rows


def add_timestamp(df):
    df["_accessed_at"] = datetime.now()
    return df


def add_ministry(df, ministry_name):
    df["ministry_name"] = ministry_name
    return df
