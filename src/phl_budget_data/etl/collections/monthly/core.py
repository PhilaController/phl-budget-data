"""Base class for monthly collections reports for the City of Philadelphia."""

import calendar
from dataclasses import dataclass
from typing import ClassVar

import pdfplumber

from ... import DATA_DIR
from ...etl import ETLPipeline
from ...utils.misc import fiscal_from_calendar_year
from ...utils.transformations import *


def get_column_names(month: int, calendar_year: int) -> List[str]:
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


@dataclass
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

    report_type: ClassVar[str]
    month: int
    year: int

    def __post_init__(self):
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
    def get_data_directory(cls, kind: str) -> str:
        """Internal function to get the file path.

        Parameters
        ----------
        kind : {'raw', 'processed'}
            type of data to load
        """
        assert kind in ["raw", "processed"]

        return DATA_DIR / kind / "collections" / "monthly" / f"{cls.report_type}"

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform the raw parsing data into a clean data frame."""

        # Apply each of the transformations
        data = (
            data.pipe(remove_footnotes)
            .pipe(fix_duplicated_chars)
            .pipe(remove_spaces)
            .pipe(fix_duplicate_parens)
            .pipe(fix_percentages)
            .pipe(replace_missing_cells)
            .pipe(remove_extra_letters, col_num=1)
            .pipe(convert_to_floats, usecols=data.columns[1:])
            .pipe(remove_missing_rows, usecols=data.columns[1:])
        ).reset_index(drop=True)

        return data
