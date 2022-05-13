from pathlib import Path
from typing import Iterable, Literal

import pandas as pd
from pydantic import validate_arguments

from .summary import ActualDepartmentSpending, BudgetedDepartmentSpending

__all__ = ["load_budgeted_department_spending", "load_actual_department_spending"]


def _load_and_combine_csv_files(files: Iterable[Path]) -> pd.DataFrame:
    """Internal function to load and combine CSV files."""

    out = []
    for f in sorted(files):
        out.append(pd.read_csv(f, dtype={"dept_code": str, "dept_major_code": str}))

    return pd.concat(out, ignore_index=True).drop_duplicates(
        subset=["dept_code", "fiscal_year"], keep="first"
    )


@validate_arguments
def load_budgeted_department_spending(
    kind: Literal["adopted", "proposed"]
) -> pd.DataFrame:
    """
    Load historical budgeted spending by department and major class.

    Sources
    -------
    - Annual Budget-in-Brief documents
    - https://www.phila.gov/departments/office-of-the-director-of-finance/financial-reports/#/budget-in-brief

    Parameters
    ----------
    kind :
        Either the adopted or proposed budget data

    Returns
    -------
    data :
        The budgeted department data
    """
    # Get the data files to load
    data_folder = (
        BudgetedDepartmentSpending.get_data_directory("processed") / kind / "budget"
    )
    files = data_folder.glob("*csv")

    # Return combined data
    return _load_and_combine_csv_files(files)


def load_actual_department_spending() -> pd.DataFrame:
    """
    Load historical actual spending by department and major class.

    Sources
    -------
    - Annual Budget-in-Brief documents
    - https://www.phila.gov/departments/office-of-the-director-of-finance/financial-reports/#/budget-in-brief

    Returns
    -------
    data :
        The actual department data
    """
    # Get the folder to load data from
    data_folder = ActualDepartmentSpending.get_data_directory("processed")

    # Get the files to load
    # NOTE: This load from both "adopted" and "proposed" budget in briefs since
    #       actual value are the same in both
    files = data_folder.glob("*/actual/*.csv")

    # Return combined data
    return _load_and_combine_csv_files(files)
