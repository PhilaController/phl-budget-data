"""Class for parsing the Departmental Obligations Report from the QCMR."""

import datetime
from typing import ClassVar

import pandas as pd
from pydantic import BaseModel, Field, validator

from ...core import validate_data_schema
from ...utils.depts import merge_department_info
from ...utils.misc import get_index_label
from ...utils.transformations import convert_to_floats, decimal_to_comma, replace_commas
from ..base import ETLPipelineQCMR, add_as_of_date


class DepartmentObligationsSchema(BaseModel):
    """Schema for the Department Obligations Report from the QCMR."""

    total: int = Field(
        title="Total Department Obligations",
        description="The total department obligations.",
    )
    fiscal_year: int = Field(title="Fiscal Year", description="The fiscal year.")
    variable: str = Field(
        title="Variable",
        description="The variable type; one of 'Actual', 'Target Budget', 'Adopted Budget', 'Current Projection'",
    )
    time_period: str = Field(
        title="Time Period",
        description="The time period for the variable, either 'Full Year' or 'YTD'.",
    )
    dept_name_raw: str = Field(
        title="Department Name (Raw)",
        description="The raw department name parsed from the report.",
    )
    dept_code: str = Field(
        title="Department Code",
        description="The two-digit department code.",
        min_length=2,
    )
    abbreviation: str = Field(
        title="Department Abbreviation", description="The department abbreviation."
    )
    dept_name: str = Field(
        title="Department Name", description="The name of the department."
    )
    as_of_date: datetime.date = Field(
        title="As of Date", description="The date of the value."
    )

    @validator("variable")
    def variable_ok(cls, variable):
        """Validate the 'variable' field."""
        options = [
            "Actual",
            "Target Budget",
            "Adopted Budget",
            "Current Projection",
        ]
        if variable not in options:
            raise ValueError(f"'variable' should be one of: {', '.join(options)}")

        return variable

    @validator("time_period")
    def time_period_ok(cls, time_period):
        """Validate the 'time_period' field."""
        options = ["Full Year", "YTD"]
        if time_period not in options:
            raise ValueError(f"'time_period' should be one of: {', '.join(options)}")

        return time_period


def remove_line_items(data, start, stop):
    """Remove the line-items from under a department."""

    # Remove line-items for benefits
    sel = data["dept_name"].str.contains(start)
    assert sel.sum() == 1

    # Figure out which ones to remove
    i = data.loc[sel].squeeze().name + 1
    remove = []
    while i < len(data) and not data.loc[i, "dept_name"].startswith(stop):
        remove.append(i)
        i += 1

    if len(remove):
        return data.drop(remove).reset_index(drop=True)
    else:
        return data


def get_column_names(fy, q):
    """Get the column names for the report."""

    cols = [f"{fy-1}-Actual-Full Year"]

    # Add YTD totals if not Q4
    if q != 4:
        cols += [
            f"{fy}-Target Budget-YTD",
            f"{fy}-Actual-YTD",
            "",
        ]

    # Add full year totals
    cols += [
        f"{fy}-Adopted Budget-Full Year",
        f"{fy}-Target Budget-Full Year",
        f"{fy}-Current Projection-Full Year",
        "",
        "",
    ]
    return cols


class DepartmentObligations(ETLPipelineQCMR):
    """
    Class for extracting data from the Departmental Obligations
    Summary Report from the City of Philadelphia's QCMR.

    Parameters
    ----------
    fiscal_year :
        the fiscal year
    quarter : int
        the fiscal quarter
    """

    dtype: ClassVar[str] = "obligations"

    def extract(self) -> pd.DataFrame:
        """Extract the contents of the PDF."""

        # Get the Textract output
        out = []
        for pg_num in range(1, self.num_pages + 1):

            # Parse the page
            df = self._get_textract_output(pg_num=pg_num)

            # Trim header
            start = get_index_label(df, "DEPARTMENT")
            df = df.loc[start:].iloc[1:]  # Remove header

            # Save
            out.append(df)

        return pd.concat(out).dropna(how="all")

    @validate_data_schema(data_schema=DepartmentObligationsSchema)
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform the raw parsing data into a clean data frame."""

        # Apply each of the transformations
        data = (
            data.pipe(decimal_to_comma, usecols=data.columns[1:])
            .pipe(replace_commas, usecols=data.columns[1:])
            .pipe(convert_to_floats, usecols=data.columns[1:])
            .fillna(0)
            .rename(columns={"0": "dept_name"})
            .reset_index(drop=True)
            .assign(dept_name=lambda df: df["dept_name"].astype(str).str.strip())
        )

        # Check columns
        ncol = len(data.columns)
        expected_ncol = 10 if self.quarter != 4 else 7
        if ncol != expected_ncol:
            raise ValueError(
                f"Unexpected number of columns: got {ncol}, expected {expected_ncol}"
            )

        # Assign columns
        column_names = get_column_names(self.fiscal_year, self.quarter)
        data = data.rename(columns=dict(zip(data.columns[1:], column_names))).drop(
            columns=[""]
        )

        for (start, stop) in [
            ("Employee Benefits", "Finance"),
            ("Public Health", "Public Property"),
            ("Human Services", "Labor"),
            ("First Judicial", "Fleet"),
        ]:
            data = remove_line_items(data, start, stop)

        # Pivot the data
        data = data.melt(id_vars=["dept_name"], value_name="total", var_name="temp")
        data = (
            data.join(
                pd.DataFrame(
                    data["temp"].apply(lambda x: x.split("-")).tolist(),
                    columns=["fiscal_year", "variable", "time_period"],
                )
            )
            .drop(columns=["temp"])
            .assign(fiscal_year=lambda df_: df_.fiscal_year.astype(int))
        )

        # Assign as-of date
        data["as_of_date"] = data.apply(add_as_of_date, args=(self,), axis=1)

        # Get general fund
        general_fund = data["dept_name"].str.lower().str.contains("general fund")

        # Save for validation
        self.validation = data.loc[general_fund]

        # Now remove it
        data = data.loc[~general_fund]

        # Get dept info and merge
        # NOTE: this will open a command line app in textual if missing exist
        dept_info = merge_department_info(data[["dept_name"]].drop_duplicates())
        return (
            data.rename(columns={"dept_name": "dept_name_raw"})
            .merge(dept_info, on="dept_name_raw", how="left")
            .drop(columns=["alias"])
        )

    def validate(self, data):
        """Validate the input data."""

        if not hasattr(self, "validation"):
            raise ValueError("Please call transform() first")

        citywide = data.groupby(["variable", "fiscal_year", "time_period"])[
            "total"
        ].sum()
        total = self.validation.set_index(citywide.index.names)["total"]

        diff = citywide - total
        if not (diff == 0).all():
            assert False

        return True
