"""Module for add department info to input data."""
import json

import pandas as pd
from billy_penn.departments import load_city_departments

from ... import ETL_DATA_DIR
from .selector import launch_selector


def add_department_info(
    data: pd.DataFrame,
    left_on: str = "dept_name",
    right_on: str = "alias",
    match_missing: bool = True,
) -> pd.DataFrame:
    """
    Add department info to the input data.

    Parameters
    ----------
    data :
        The input dataframe.
    left_on :
        The column in the input data to merge on
    right_on :
        The column in the dept info data to merge on
    match_missing :
        Whether to attempt to match missing departments.
    """
    # Load the department info with aliases and subitems
    dept_info = load_city_departments(include_aliases=True, include_line_items=True)

    # Merge into the info
    data = data.merge(
        dept_info,
        left_on=left_on,
        right_on=right_on,
        how="left",
        validate="1:1",
        suffixes=("_raw", ""),
    )

    # Match missing departments
    if match_missing:
        data = match_missing_departments(data)

    return data


def match_missing_departments(data: pd.DataFrame) -> pd.DataFrame:
    """
    Match missing departments.

    This launches a text-based selector to match missing departments.
    """

    # Make a copy
    data = data.copy()

    # Check for missing
    missing = data["alias"].isnull()
    if missing.sum():

        # Get the missing dept names and exclude general fund
        missing_depts = data.loc[missing]["dept_name_raw"].tolist()
        missing_depts = [d for d in missing_depts if "general fund" not in d.lower()]

        # Load any cached matches
        filename = ETL_DATA_DIR / "interim" / "dept-matches.json"
        with filename.open("r") as ff:
            cached_matches = json.load(ff)

        # Find matches
        if len(missing_depts):

            # Get the options
            depts_df = load_city_departments(include_line_items=True)
            depts = sorted(depts_df["dept_name"])

            # Find a match for each missing department
            for missing_dept in missing_depts:

                if missing_dept in cached_matches:
                    matched_dept = cached_matches[missing_dept]
                else:
                    # Launch the selector
                    matched_dept = launch_selector(depts, missing_dept)

                    # Raise an error if no match
                    if matched_dept is None:
                        raise ValueError(
                            f"Missing aliases for {len(missing_depts)} departments:\n{missing_depts}"
                        )
                    else:
                        sel = depts_df["dept_name"] == matched_dept
                        matched_dept = depts_df.loc[sel].squeeze().to_dict()

                    # Save it
                    cached_matches[missing_dept] = matched_dept

                # Update the values
                sel = data["dept_name_raw"] == missing_dept
                for col, value in matched_dept.items():
                    data.loc[sel, col] = value

            # Save the cached matches
            with filename.open("w") as ff:
                json.dump(cached_matches, ff)

    return data
