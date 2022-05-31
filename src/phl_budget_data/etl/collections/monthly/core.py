"""Base class for monthly collections reports for the City of Philadelphia."""

import calendar
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar, Literal

import pandas as pd
import pdfplumber

from ... import ETL_DATA_DIR, ETL_DATA_FOLDERS
from ...core import ETLPipeline
from ...utils import transformations as tr
from ...utils.misc import fiscal_from_calendar_year


def get_column_names(month: int, calendar_year: int) -> list[str]:
    """Columns for monthly collection reports."""

    # Get the FY from the calendar year
    fiscal_year = fiscal_from_calendar_year(month, calendar_year)

    # Get the month name
    month_name = calendar.month_abbr[month].lower()

    # Fiscal year tsags
    this_year = f"fy{str(fiscal_year)[2:]}"
    last_year = f"fy{str(fiscal_year-1)[2:]}"

    return [
        f"{last_year}_actual",
        f"{this_year}_budgeted",
        f"{month_name}_{this_year}",
        f"{month_name}_{last_year}",
        f"{this_year}_ytd",
        f"{last_year}_ytd",
        "net_change",
        "budget_requirement",
        "pct_budgeted",
    ]


# Either City or School collections
COLLECTION_TYPES = Literal["city", "school"]


@dataclass  # type: ignore
class MonthlyCollectionsReport(ETLPipeline):
    """
    Base class for extracting data from the City of Philadelphia's
    monthly collections PDF reports.

    Parameters
    ----------
    month :
        the calendar month number (starting at 1)
    year :
        the calendar year
    """

    report_type: ClassVar[COLLECTION_TYPES]
    month: int
    year: int

    def __post_init__(self) -> None:
        """Set up necessary variables."""

        # The PDF path
        dirname = self.get_data_directory("raw")
        self.path = dirname / f"{self.year}_{self.month:02d}.pdf"

        # Make sure this path exists
        if not self.path.exists():
            raise FileNotFoundError(
                f"No PDF available for month '{self.month}' and year '{self.year}' at '{self.path}'"
            )

        # Get the number of pages
        with pdfplumber.open(self.path) as pdf:
            self.num_pages = len(pdf.pages)

        # Month name
        self.month_name = calendar.month_abbr[self.month].lower()

    @classmethod
    def get_data_directory(cls, kind: Literal[ETL_DATA_FOLDERS]) -> Path:
        """Internal function to get the file path.

        Parameters
        ----------
        kind : {'raw', 'processed'}
            type of data to load
        """
        return ETL_DATA_DIR / kind / "collections" / "monthly" / f"{cls.report_type}"

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform the raw parsing data into a clean data frame."""

        # Apply each of the transformations
        data = (
            data.pipe(tr.remove_footnotes)
            .pipe(tr.fix_duplicated_chars)
            .pipe(tr.remove_spaces)
            .pipe(tr.fix_duplicate_parens)
            .pipe(tr.fix_percentages)
            .pipe(tr.replace_missing_cells)
            .pipe(tr.remove_extra_letters, col_num=1)
            .pipe(tr.convert_to_floats, usecols=data.columns[1:])
            .pipe(tr.remove_missing_rows, usecols=data.columns[1:])
        ).reset_index(drop=True)

        # Remove any net accrual columns
        if len(data.columns) == 14:
            data = data.drop(labels=[7, 8, 9, 10], axis=1)
            data.columns = list(range(len(data.columns)))

        return data
