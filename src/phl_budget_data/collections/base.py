"""Base class for monthly collections reports for the City of Philadelphia."""

import calendar
from dataclasses import dataclass
from typing import ClassVar

import pdfplumber

from .. import DATA_DIR
from ..base import ETLPipeline
from ..utils.transformations import *


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

    Attributes
    ----------
    path : Path
        the name of the path to the raw PDF file
    num_pages : int
        the number of pages in the PDF
    legacy : bool
        whether the PDF uses a legacy format
    month_name : str
        the 3-letter abbreviation for the month name
    """

    report_type: ClassVar[str]
    month: int
    year: int

    def __post_init__(self):
        """Set up necessary variables."""

        # The PDF path
        dirname = self._get_data_directory("raw")
        self.path = dirname / f"{self.year}_{self.month:02d}.pdf"

        # Make sure this path exists
        if not self.path.exists():
            raise FileNotFoundError(
                f"No PDF available for month '{self.month}' and year '{self.year}'"
            )

        # Get the number of pages
        with pdfplumber.open(self.path) as pdf:
            self.num_pages = len(pdf.pages)

        # Month name
        self.month_name = calendar.month_abbr[self.month].lower()

    def _get_data_directory(self, kind: str) -> str:
        """Internal function to get the file path.

        Parameters
        ----------
        kind : {'raw', 'processed'}
            type of data to load
        """
        assert kind in ["raw", "processed"]

        return DATA_DIR / kind / "collections" / f"{self.report_type}-monthly"

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

        # Remove the first row if we need to
        if data.iloc[0][0].startswith("GENERAL"):
            data = data.iloc[1:]

        return data
