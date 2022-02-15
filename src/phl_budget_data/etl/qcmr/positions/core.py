"""Class for parsing the Full-Time Positions Report from the QCMR."""

import datetime
from typing import ClassVar, Optional

import pandas as pd
from pydantic import BaseModel, Field, validator

from ...core import validate_data_schema
from ...utils.depts import merge_department_info
from ...utils.misc import get_index_label
from ...utils.transformations import convert_to_floats, decimal_to_comma, fix_zeros
from ..base import ETLPipelineQCMR

UNIFORMED = ["Police", "Fire", "District Attorney"]


class FullTimePositionsSchema(BaseModel):
    """Schema for the Full-Time Positions Report from the QCMR."""

    civilian: int = Field(
        title="Full-Time Civilian Positions",
        description="The number of full-time civilian positions.",
    )
    uniformed: int = Field(
        title="Full-Time Uniformed Positions",
        description="The number of full-time uniformed positions.",
    )
    total: int = Field(
        title="Total Full-Time Positions",
        description="The total number of full-time positions.",
    )
    fiscal_year: int = Field(title="Fiscal Year", description="The fiscal year.")
    variable: str = Field(
        title="Variable",
        description="The variable type, either 'Actual' or 'Adopted Budget'",
    )
    time_period: str = Field(
        title="Time Period",
        description="The time period for the variable, either 'Full Year' or 'YTD'.",
    )
    fund: str = Field(
        title="Fund",
        description="The name of the fund, either 'General', 'Other', or 'Total'.",
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
    as_of_date: Optional[datetime.date] = Field(
        title="As of Date", description="The date of the value."
    )

    @validator("fund")
    def variable_fund(cls, fund):
        """Validate the 'fund' field."""
        options = ["Other", "Total", "General"]
        if fund not in options:
            raise ValueError(f"'fund' should be one of: {', '.join(options)}")

        return fund

    @validator("variable")
    def variable_ok(cls, variable):
        """Validate the 'variable' field."""
        options = [
            "Actual",
            "Adopted Budget",
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


def _to_tidy_data(df, cols):
    """Pivot data to a tidy format for the specified columns."""

    remove = []
    uniformed = []
    for tag in UNIFORMED:
        sel = df["0"].str.startswith(tag)
        i = df.loc[sel].squeeze().name

        x = df.loc[i : i + 2].copy()
        remove += x.index.tolist()
        x = x.iloc[1:]
        x["dept_name"] = tag
        uniformed.append(_transform_uniformed_depts(x, cols))

    # Make into a dataframe
    uniformed = pd.concat(uniformed, axis=0, ignore_index=True)

    # Remove the specified rows
    df2 = df.drop(remove)

    # Combine
    out = pd.concat(
        [
            df2[cols]
            .rename(
                columns={
                    cols[0]: "dept_name",
                    cols[1]: "General",
                    cols[2]: "Other",
                    cols[3]: "Total",
                }
            )
            .melt(id_vars=["dept_name"], value_name="civilian", var_name="fund")
            .assign(uniformed=0),
            uniformed,
        ]
    )

    return out


def _transform_uniformed_depts(x, cols):
    """Transform data for uniformed departments."""

    XX = (
        x[["dept_name"] + cols]
        .assign(**{"0": ["civilian", "uniformed"]})
        .rename(columns={cols[1]: "General", cols[2]: "Other", cols[3]: "Total"})
        .pivot(columns="0", index="dept_name")
    )

    return (
        XX.swaplevel(axis=1)
        .stack(level=1)
        .rename_axis(["dept_name", "fund"])
        .reset_index()
    )


class FullTimePositions(ETLPipelineQCMR):
    """
    The Full-Time Positions Report from the QCMR.

    Parameters
    ----------
    fiscal_year :
        the fiscal year
    quarter : int
        the fiscal quarter
    """

    dtype: ClassVar[str] = "positions"

    def extract(self) -> pd.DataFrame:
        """Extract the contents of the PDF."""

        # Get the Textract output
        out = []
        for pg_num in range(1, self.num_pages + 1):

            # Parse the page
            df = self._get_textract_output(pg_num=pg_num)

            # Trim header
            start = get_index_label(df, "Department")
            df = df.loc[start:].iloc[1:]  # Remove header

            # Save
            out.append(df)

        return pd.concat(out).dropna(how="all")

    @validate_data_schema(data_schema=FullTimePositionsSchema)
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform the raw parsing data into a clean data frame."""

        # Check the number of columns
        assert len(data.columns) == 11

        # Reset the index back to range
        data = data.reset_index(drop=True)

        # Get the three sets of columns
        as_of_dates = {
            1: f"{self.fiscal_year-1}-09-30",
            2: f"{self.fiscal_year-1}-12-31",
            3: f"{self.fiscal_year}-03-31",
            4: f"{self.fiscal_year}-06-30",
        }
        data = pd.concat(
            [
                _to_tidy_data(data, ["0", "7", "8", "9"]).assign(
                    fiscal_year=self.fiscal_year,
                    variable="Actual",
                    time_period="YTD",
                    as_of_date=as_of_dates[self.quarter],
                ),
                _to_tidy_data(data, ["0", "1", "2", "3"]).assign(
                    fiscal_year=self.fiscal_year - 1,
                    variable="Actual",
                    time_period="Full Year",
                    as_of_date=f"{self.fiscal_year-1}-06-30",
                ),
                _to_tidy_data(data, ["0", "4", "5", "6"]).assign(
                    fiscal_year=self.fiscal_year,
                    variable="Adopted Budget",
                    time_period="Full Year",
                    as_of_date=None,
                ),
            ]
        )

        data = (
            data.pipe(decimal_to_comma, usecols=["civilian", "uniformed"])
            .pipe(fix_zeros, usecols=["civilian", "uniformed"])
            .pipe(convert_to_floats, usecols=["civilian", "uniformed"])
            .reset_index(drop=True)
            .assign(
                dept_name=lambda df: df["dept_name"].astype(str).str.strip(),
                total=lambda df_: df_.civilian + df_.uniformed,
            )
        )

        # Get all funds and save it as validation
        all_funds = data["dept_name"].str.lower().str.contains("all funds")
        self.validation = data.loc[all_funds]

        # Now remove it
        data = data.loc[~all_funds]

        # Get dept info and merge it  in
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

        # Sum up the departments
        A = (
            data.groupby(["fund", "fiscal_year", "variable"])[["civilian", "uniformed"]]
            .sum()
            .sum(axis=1)
        )

        # All funds total
        B = (
            self.validation[["fund", "fiscal_year", "variable", "civilian"]]
            .set_index(A.index.names)
            .squeeze()
        )

        diff = A - B
        if not (diff == 0).all():
            print(diff)
            assert False

        return True
