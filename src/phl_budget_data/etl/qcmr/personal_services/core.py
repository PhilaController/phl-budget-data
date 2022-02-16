"""Class to parse the Personal Services Summary from the QCMR."""
import datetime
from typing import ClassVar, Literal

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field

from ...core import validate_data_schema
from ...utils.depts import merge_department_info
from ...utils.transformations import convert_to_floats, decimal_to_comma, fix_zeros
from ..base import ETLPipelineQCMR, add_as_of_date


class PersonalServicesSchema(BaseModel):
    """Schema for the Personal Services Summary from the QCMR."""

    full_time_positions: int = Field(
        title="Full-Time Positions", description="The full-time employee positions."
    )
    class_100_total: int = Field(
        title="Class 100 Total", description="The class 100 payroll total."
    )
    class_100_ot: int = Field(
        title="Class 100 Overtime", description="The class 100 overtime total."
    )
    fiscal_year: int = Field(title="Fiscal Year", description="The fiscal year.")
    variable: Literal[
        "Actual",
        "Target Budget",
        "Adopted Budget",
        "Current Projection",
    ] = Field(
        title="Variable",
        description="The spending variable type; one of 'Actual', 'Target Budget', 'Adopted Budget', 'Current Projection'",
    )
    time_period: Literal["Full Year", "YTD"] = Field(
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


def _to_tidy_format(X, fy, qtr):
    """
    Utility function to pivot the data to a tidy format.

    This takes wide-form data for a specific department, and
    pivots it to a long-form dataframe.
    """
    # This is the department name
    dept = X["0"].iloc[0]

    # There must be four rows
    if len(X) != 4:

        if dept.startswith("TOTAL GENERAL FUND"):
            dept = "TOTAL GENERAL FUND"
        else:
            raise ValueError(
                f"Data for department '{dept}' has length {len(X)}, expected 4."
            )

    # First column of first row should not be empty, all else should be empty
    if len(X) == 4:
        test = X.iloc[0][X.columns[1:]].isnull()
        if not test.all():
            print(X[X.columns[0]])
            raise ValueError(
                f"Data for department '{dept}' has non-empty values in the first row."
            )

    # Remove the first empty row
    if len(X) == 4:
        X = X.iloc[1:]

    # Rename the columns
    if qtr != 4 or fy <= 2010:
        X.columns = [
            "Variable",
            f"{fy-3}-Actual-Full Year",
            f"{fy-2}-Actual-Full Year",
            f"{fy-1}-Actual-Full Year",
            f"{fy}-Target Budget-YTD",
            f"{fy}-Actual-YTD",
            "",
            f"{fy}-Adopted Budget-Full Year",
            f"{fy}-Target Budget-Full Year",
            f"{fy}-Current Projection-Full Year",
            "",
            "",
        ]
    else:
        X.columns = [
            "Variable",
            f"{fy-3}-Actual-Full Year",
            f"{fy-2}-Actual-Full Year",
            f"{fy-1}-Actual-Full Year",
            f"{fy}-Adopted Budget-Full Year",
            f"{fy}-Target Budget-Full Year",
            f"{fy}-Current Projection-Full Year",
            "",
            "",
        ]

    # Drop the empty columns
    X = X.drop(columns=[""])

    # Transpose and rename the columns
    XX = X.set_index("Variable").T
    XX.columns = ["full_time_positions", "class_100_total", "class_100_ot"]
    XX = XX.reset_index().rename(columns={"index": "temp"})

    # Pivot the data into tidy format
    return (
        XX.join(
            pd.DataFrame(
                XX["temp"].apply(lambda x: x.split("-")).tolist(),
                columns=["fiscal_year", "variable", "time_period"],
            )
        )
        .drop(columns=["temp"])
        .assign(dept_name=dept, fiscal_year=lambda df_: df_.fiscal_year.astype(int))
    )


class PersonalServices(ETLPipelineQCMR):
    """
    The Personal Services Summary from the QCMR.

    Parameters
    ----------
    fiscal_year :
        the fiscal year
    quarter :
        the fiscal quarter
    """

    dtype: ClassVar[str] = "personal-services"

    def extract(self) -> pd.DataFrame:
        """Extract the contents of the PDF using AWS textract."""

        out = []
        for pg_num in range(1, self.num_pages + 1):
            out.append(self._get_textract_output(pg_num=pg_num))

        return pd.concat(out).dropna(how="all")

    @validate_data_schema(data_schema=PersonalServicesSchema)
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform the raw parsing data into a clean data frame."""

        # Remove header rows from each page
        data = data.loc[
            ~(data["0"].isnull()) & (~data["0"].str.startswith("Department", na=False))
        ].copy()

        # Remove footnotes
        footnotes = [
            "*DHS expenses are transferred from the Grants Fd.",
            "*DHS expenses are transferred from the Grants",
            "*Police OT is abated as reimbursements occur",
        ]
        for footnote in footnotes:
            data["0"] = (
                data["0"]
                .str.replace(footnote, "", regex=False)
                .str.strip()
                .replace("", np.nan)
            )

        # Remove null rows
        data = data.dropna(how="all")

        # Remove any other footnotes
        data = data.loc[~data["0"].str.startswith("*", na=False)]

        # Loop through each department, 4 rows at a time
        start = 0
        stop = start + 4
        out = []
        while start < len(data):

            # Get data for this department
            df_this_dept = data.iloc[start:stop]

            # Pivot the data into
            out.append(_to_tidy_format(df_this_dept, self.fiscal_year, self.quarter))

            # Increment the start and stop
            start = stop
            stop += 4

        # Combine into a single dataframe
        data = pd.concat(out, ignore_index=True)

        # Drop any sub-departments
        sub_departments = [
            "OIT-Base",
            "OIT-911",
            "Administration & Management",
            "Performance Mgmt. & Accountability",
            "Juvenile Justice Services",
            "Children & Youth",
            "Community Based Prevention Services",
            "Ambulatory Health Services",
            "Early Childhood, Youth & Women's Hlth.",
            "Phila. Nursing Home",
            "Environmental Protection Services",
            "Administration and Support Svcs.",
            "Contract Admin. and Program Evaluation",
            "Aids Activities Coordinating Office",
            "Medical Examiner's Office",
            "Infectious Disease Control",
            "Chronic Disease Control",
            "Chronic Disease",
            "Sanitation",
            "Transportation",
            "Engineering Design & Surveying",
            "Highways",
            "Street Lighting",
            "Traffic Engineering",
            "General Support",
            "Common Pleas Court",
            "Court Administrator",
            "Municipal Court",
            "Traffic Court",
        ]
        data = data.loc[~data["dept_name"].isin(sub_departments)]

        # Transform the columns
        cols = ["full_time_positions", "class_100_total", "class_100_ot"]
        data = (
            data.pipe(decimal_to_comma, usecols=cols)
            .pipe(fix_zeros, usecols=cols)
            .pipe(convert_to_floats, usecols=cols)
            .reset_index(drop=True)
            .assign(dept_name=lambda df: df["dept_name"].astype(str).str.strip())
        )

        # Assign as-of date
        data["as_of_date"] = data.apply(add_as_of_date, args=(self,), axis=1)

        # Get the total for the General Fund and save as the validation
        general_fund = data["dept_name"].str.lower().str.contains("general fund")
        self.validation = data.loc[general_fund]

        # Now remove the validation
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
        """Validate the totals."""

        if not hasattr(self, "validation"):
            raise ValueError("Please call transform() first")

        # Sum over all departments
        A = data.groupby(["fiscal_year", "variable", "time_period"])[
            ["full_time_positions", "class_100_total", "class_100_ot"]
        ].sum()

        # General Fund values
        B = self.validation.set_index(A.index.names)[A.columns]

        # Difference
        diff = A - B
        assert (diff == 0).all().all()

        return True
