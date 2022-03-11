from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

import pandas as pd
import pdfplumber
from loguru import logger

from ... import ETL_DATA_DIR as DATA_DIR
from ..core import ETLPipeline
from ..utils.depts import merge_department_info
from ..utils.pdf import extract_words, words_to_table
from ..utils.transformations import *

MAJOR_CLASS_NAMES = {
    "Total": "total",
    "Purchase of Services": "class_200",
    "Personal Services": "class_100",
    "Materials, Supplies & Equip.": "class_300_400",
    "Contrib., Indemnities & Taxes": "class_500",
    "Contrib. indemnities & Taxes": "class_500",
    "Payments to Other Funds": "class_800",
    "Advances and Other Misc. Payments": "class_900",
    "Advances & Miscellaneous Payments": "class_900",
    "Pers. Svcs.-Emp.Benefits": "class_100",
    "Pers. Svcs.-Emp.Benefit": "class_100",
    "Debt Service": "class_700",
}

CLASS_COLUMNS = [
    "class_100",
    "class_200",
    "class_300_400",
    "class_500",
    "class_700",
    "class_800",
    "class_900",
]


@dataclass
class BudgetSummaryBase(ETLPipeline):
    """
    Budget Summary.

    Parameters
    ----------
    fiscal_year :
        the fiscal year
    kind :
        either proposed or adopted
    """

    fiscal_year: int
    kind: str
    flavor: ClassVar[str] = None

    def __post_init__(self):
        """Set up necessary variables."""

        assert self.kind in ["adopted", "proposed"]

        tag = str(self.fiscal_year)[2:]
        self.path = self.get_data_directory("raw") / self.kind / f"FY{tag}.pdf"

        # Make sure this path exists
        if not self.path.exists():
            raise FileNotFoundError(
                f"No PDF available for fiscal year '{self.fiscal_year}' at '{self.path}'"
            )

        # Get the value column
        if self.flavor == "actual":
            self.value_column = f"FY{self.fiscal_year-2} Actual"
        else:
            self.value_column = f"FY{self.fiscal_year} Budgeted"

    @classmethod
    def get_data_directory(cls, kind: str) -> str:
        """Internal function to get the file path.

        Parameters
        ----------
        kind : {'raw', 'processed'}
            type of data to load
        """
        assert kind in ["raw", "processed", "interim"]

        return DATA_DIR / kind / "budget-in-brief"

    def extract(self) -> pd.DataFrame:
        """Extract the data from the first PDF page."""

        # Load and parse interim results for older budgets
        if self.fiscal_year <= 2015:

            # Get the interim path
            tag = str(self.fiscal_year)[2:]
            path = self.get_data_directory("interim") / self.kind / f"FY{tag}.xlsx"

            # Make sure it exists
            if not path.exists():
                raise ValueError("Interim parsing results not available")

            # Return
            return pd.read_excel(path, sheet_name=0).rename(
                columns={"Category": "major_class", "Department": "dept_name"}
            )

        # Parse newer PDFs directly
        with pdfplumber.open(self.path) as pdf:

            # Extract the words and convert to a table
            data = pd.concat(
                [
                    words_to_table(extract_words(pg, y_tolerance=1), min_col_sep=30)
                    .iloc[6:]
                    .dropna(axis=1, how="all")
                    for pg in pdf.pages
                ]
            ).reset_index(drop=True)

            # The total line
            total = data.iloc[-1]

            # Fix Departments that span two lines
            to_drop = []
            headers = data.index[(data[data.columns[1:]] == "").all(axis=1)]
            for i, value in enumerate(headers):
                if i != len(headers) - 1:
                    next_value = headers[i + 1]
                    if next_value == value + 1:
                        to_drop.append(next_value)
                        data.loc[value, 0] += " " + data.loc[next_value, 0]

            data = data.drop(to_drop).reset_index(drop=True)

            # Start/Stop
            starts = data.index[(data[data.columns[1:]] == "").all(axis=1)]
            stops = data.index[data[0].str.strip() == "Total"]

            out = []
            for (start, stop) in zip(starts, stops):

                df = data.loc[start:stop].copy()
                dept_name = df[0].iloc[0]

                df = (
                    df[[0, 1, 5]]
                    .iloc[1:]
                    .rename(
                        columns={
                            0: "major_class",
                            1: f"FY{self.fiscal_year-2} Actual",
                            5: f"FY{self.fiscal_year} Budgeted",
                        }
                    )
                )
                df["dept_name"] = dept_name
                out.append(df)

            out = pd.concat(out, axis=0).reset_index(drop=True)

            out["dept_name"] = (
                out["dept_name"].str.replace("\(\d\)", "", regex=True).str.strip()
            )

            return pd.concat(
                [
                    out,
                    pd.DataFrame(
                        [["Total", total[1], total[5], "General Fund"]],
                        columns=out.columns,
                    ),
                ],
                axis=0,
                ignore_index=True,
            )

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform the raw parsing data into a clean data frame."""

        # Convert to floats
        out = convert_to_floats(data, usecols=[self.value_column]).fillna(0)

        # Replace the major class values
        out["major_class"] = out["major_class"].replace(MAJOR_CLASS_NAMES)

        # Pivot
        FY = self.fiscal_year - 2 if self.flavor == "actual" else self.fiscal_year
        out = (
            out.pivot_table(
                columns="major_class",
                index="dept_name",
                values=self.value_column,
            )
            .fillna(0)
            .reset_index()
            .assign(fiscal_year=FY)
        )

        # Make sure all classes are zero
        for col in CLASS_COLUMNS:
            if col not in out.columns:
                out[col] = 0

        # Add the total
        if "total" not in out.columns:
            out["total"] = out[CLASS_COLUMNS].sum(axis=1)

        # Save the General Fund total
        general_fund = out["dept_name"].str.lower().str.contains("general fund")

        # Save for validation
        self.validation = out.loc[general_fund]

        # Now remove it
        out = out.loc[~general_fund]

        # Merge in value-added columns
        # Get dept info and merge
        # NOTE: this will open a command line app in textual if missing exist
        dept_info = merge_department_info(out[["dept_name"]].drop_duplicates())
        out = (
            out.rename(columns={"dept_name": "dept_name_raw"})
            .merge(dept_info, on="dept_name_raw", how="left")
            .drop(columns=["alias"])
        )

        # Fix Finance: Recession Reserve
        # Get a copy of finance
        finance = out.query("dept_code == '35'").copy()
        finance_900 = finance["class_900"].copy()

        # Zero out
        finance[CLASS_COLUMNS[:-1]] = 0

        # Set metadata
        finance["dept_name_raw"] = finance["abbreviation"] = "Recession Reserve"
        finance["dept_name"] = "Finance: Recession Reserve"
        finance["dept_code"] = "35-RR"
        finance["total"] = finance_900

        # Combine
        out = pd.concat([out, finance], ignore_index=True)

        # Subtract from full Finance
        sel = out["dept_code"] == "35"
        out.loc[sel, "class_900"] = 0
        out.loc[sel, "total"] -= finance_900

        # Add major dept class code
        out["dept_major_code"] = out.dept_code.str.slice(0, 2)

        return out

    def validate(self, data):
        """Validate the input data."""

        # Older validate
        if self.fiscal_year <= 2015:

            # Get the interim path
            tag = str(self.fiscal_year)[2:]
            path = self.get_data_directory("interim") / self.kind / f"FY{tag}.xlsx"

            totals = pd.read_excel(path, sheet_name=1).rename(
                columns={"Category": "major_class"}
            )
            totals["major_class"] = totals["major_class"].replace(MAJOR_CLASS_NAMES)
            totals = totals.groupby("major_class")[self.value_column].sum()

            # Test each class
            class_diff = (data[CLASS_COLUMNS].sum() - totals).dropna()
            assert (class_diff == 0).all()

        else:

            # Make sure we have all of the class columns
            assert all(col in data.columns for col in CLASS_COLUMNS)

            # Depts only
            depts = data.set_index("dept_name")

            # General Fund
            general_fund = self.validation

            # Check dept class totals equal total column
            total = depts[CLASS_COLUMNS].sum(axis=1)
            assert ((total - depts["total"]) == 0).all()

            # Check general fund total equals total column
            assert total.sum() == general_fund.squeeze()["total"]

        return True

    def load(self, data) -> None:
        """Load the data."""

        # Get the path
        tag = str(self.fiscal_year)[2:]
        path = (
            self.get_data_directory("processed")
            / self.kind
            / self.flavor
            / f"FY{tag}.csv"
        )

        # Load
        super()._load_csv_data(data, path)

    @classmethod
    def extract_transform_load_all(cls, fresh=False):
        """Run the ETL pipeline on all raw PDF files."""

        # Loop over all raw PDF files
        for pdf_path in cls.get_pdf_files():

            # Get fiscal year
            fy = int("20" + pdf_path.stem[2:])

            # Get the output path
            output_path = Path(
                str(pdf_path).replace("raw", "processed").replace(".pdf", ".csv")
            )

            # Run the ETL if we need to
            if (
                fresh
                or not output_path.exists()
                or output_path.stat().st_mtime < pdf_path.stat().st_mtime
            ):

                # Initialize and run the ETL pipeline
                logger.info(f"Running ETL for FY{fy}")

                kind = pdf_path.parts[-2]  # adopted or proposed
                etl = cls(fy, kind)
                etl.extract_transform_load()


class BudgetedDepartmentSpending(BudgetSummaryBase):
    """Budgeted spending by department by major class."""

    flavor = "budget"


class ActualDepartmentSpending(BudgetSummaryBase):
    """Actual spending by department by major class."""

    flavor = "actual"
