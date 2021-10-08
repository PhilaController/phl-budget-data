from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pdfplumber

from ... import DATA_DIR
from ..etl import ETLPipeline
from ..utils.pdf import extract_words, words_to_table
from ..utils.transformations import *

this_dir = Path(__file__).parent.absolute()
LOOKUP = pd.read_excel(this_dir / "data" / "dept_names_lookup.xlsx")


@dataclass
class BudgetSummary(ETLPipeline):
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

    @classmethod
    def get_data_directory(cls, kind: str) -> str:
        """Internal function to get the file path.

        Parameters
        ----------
        kind : {'raw', 'processed'}
            type of data to load
        """
        assert kind in ["raw", "processed"]

        return DATA_DIR / kind / "budget-in-brief"

    def extract(self) -> pd.DataFrame:
        """Extract the data from the first PDF page."""

        with pdfplumber.open(self.path) as pdf:

            data = pd.concat(
                [
                    words_to_table(extract_words(pg, y_tolerance=1), min_col_sep=30)
                    .iloc[6:]
                    .dropna(axis=1, how="all")
                    for pg in pdf.pages
                ]
            ).reset_index(drop=True)

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

            return out.append(
                pd.Series(
                    ["Total", total[1], total[5], "General Fund"], index=out.columns
                ),
                ignore_index=True,
            )

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform the raw parsing data into a clean data frame."""

        cols = [f"FY{self.fiscal_year-2} Actual", f"FY{self.fiscal_year} Budgeted"]
        out = convert_to_floats(data, usecols=cols).fillna(0)

        out["major_class"] = out["major_class"].replace(
            {
                "Total": "total",
                "Purchase of Services": "class_200",
                "Personal Services": "class_100",
                "Materials, Supplies & Equip.": "class_300_400",
                "Contrib., Indemnities & Taxes": "class_500",
                "Payments to Other Funds": "class_800",
                "Advances and Other Misc. Payments": "class_900",
                "Pers. Svcs.-Emp.Benefits": "class_100",
                "Debt Service": "class_700",
            }
        )

        # Remove totals
        # out = out.query("major_class != 'total'")

        # Pivot
        out = (
            out.pivot_table(
                columns="major_class",
                index="dept_name",
                values=f"FY{self.fiscal_year} Budgeted",
            )
            .fillna(0)
            .reset_index()
            .assign(fiscal_year=self.fiscal_year)
        )
        # Merge in value-added columns
        out = pd.merge(out, LOOKUP, how="left", on="dept_name", validate="1:1")

        return out

    # def validate(self, data):
    #     """Validate the input data."""

    #     sub = data.query("sector in ['Residential', 'Non-Residential', 'Unclassified']")
    #     total = data.query("sector == 'Total'")["total"].squeeze()

    #     diff = sub["total"].sum() - total
    #     assert diff < 5

    #     return True

    def load(self, data) -> None:
        """Load the data."""

        # Get the path
        tag = str(self.fiscal_year)[2:]
        path = self.get_data_directory("processed") / self.kind / f"FY{tag}.pdf"

        # Load
        super()._load_csv_data(data, path)
