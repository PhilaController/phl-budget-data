"""Base class for parsing the Quarterly City Manager's Report."""

from dataclasses import dataclass
from typing import ClassVar

import pdfplumber
from loguru import logger

from ... import DATA_DIR
from ..core import ETLPipelineAWS
from ..utils.misc import fiscal_year_quarter_from_path


def add_as_of_date(row, etl):
    """Add the date corresponding to the value for each row."""

    fy = row["fiscal_year"]
    if fy < etl.fiscal_year:
        assert row["time_period"] == "Full Year"
        return f"{fy}-06-30"
    else:
        as_of_dates = {
            1: f"{etl.fiscal_year-1}-09-30",
            2: f"{etl.fiscal_year-1}-12-31",
            3: f"{etl.fiscal_year}-03-31",
            4: f"{etl.fiscal_year}-06-30",
        }
        return as_of_dates[etl.quarter]


@dataclass
class ETLPipelineQCMR(ETLPipelineAWS):
    """
    Base class for extracting data from the City of Philadelphia's QCMR.

    Parameters
    ----------
    fiscal_year :
        the fiscal year
    quarter : int
        the fiscal quarter
    """

    dtype: ClassVar[str]
    fiscal_year: int
    quarter: int

    def __post_init__(self):
        """Set up necessary variables."""

        # The PDF path
        tag = str(self.fiscal_year)[2:]
        dirname = self.get_data_directory("raw")
        self.path = dirname / f"FY{tag}_Q{self.quarter}.pdf"

        # Make sure this path exists
        if not self.path.exists():
            raise FileNotFoundError(
                f"No PDF available for quarter '{self.quarter}' and fiscal year '{self.fiscal_year}' at '{self.path}'"
            )

        # Number of pages
        with pdfplumber.open(self.path) as pdf:
            self.num_pages = len(pdf.pages)

    @classmethod
    def get_data_directory(cls, kind: str) -> str:
        """Internal function to get the file path.

        Parameters
        ----------
        kind : {'raw', 'interim', 'processed'}
            type of data to load
        """
        assert kind in ["raw", "interim", "processed"]
        return DATA_DIR / kind / "qcmr" / cls.dtype

    def load(self, data) -> None:
        """Load the data."""

        # Get the path
        dirname = self.get_data_directory("processed")
        tag = str(self.fiscal_year)[2:]
        path = dirname / f"FY{tag}-Q{self.quarter}.csv"

        # Load
        super()._load_csv_data(data, path)

    @classmethod
    def extract_transform_load_all(cls, fresh=False):
        """Run the ETL pipeline on all raw PDF files."""

        # Loop over all raw PDF files
        for pdf_path in cls.get_pdf_files():

            # Get fiscal year and quarter
            fy, q = fiscal_year_quarter_from_path(pdf_path)

            # Get the output path
            dirname = cls.get_data_directory("processed")
            tag = str(fy)[2:]
            output_path = dirname / f"FY{tag}-Q{q}.csv"

            # Run the ETL if we need to
            if (
                fresh
                or not output_path.exists()
                or output_path.stat().st_mtime < pdf_path.stat().st_mtime
            ):

                # Initialize and run the ETL pipeline
                logger.info(f"Running ETL for FY{fy} Q{q}")
                etl = cls(fy, q)
                etl.extract_transform_load()
