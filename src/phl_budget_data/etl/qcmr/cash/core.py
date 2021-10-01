"""Base class for parsing the Cash Flow Forecast from the QCMR."""

import unicodedata
from dataclasses import dataclass
from typing import ClassVar

import pandas as pd
import pdfplumber

from ... import DATA_DIR
from ...etl import ETLPipeline
from ...utils.pdf import get_pdf_words
from ...utils.transformations import (
    convert_to_floats,
    remove_parentheses,
    remove_unwanted_chars,
)


@dataclass
class CashFlowForecast(ETLPipeline):
    """
    Base class for extracting data from the City of Philadelphia's
    QCMR Cash Flow Forecast.

    Parameters
    ----------
    fiscal_year :
        the fiscal year
    quarter : int
        the fiscal quarter
    """

    report_type: ClassVar[str]
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

        # Parse the words
        self.words = get_pdf_words(
            self.path,
            y_tolerance=0,
            keep_blank_chars=True,
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

        if kind == "raw":
            return DATA_DIR / kind / "qcmr" / "cash"
        else:
            return DATA_DIR / kind / "qcmr" / "cash" / f"{cls.report_type}"

    def _extract_from_page(self, pg_num, bbox=None):
        """Internal function to extract from page within a bbox."""

        # Open the PDF
        with pdfplumber.open(self.path) as pdf:

            # Get the cropped page
            pg = pdf.pages[pg_num]
            if bbox is not None:
                if bbox[2] is None:
                    bbox[2] = pg.width
                pg = pg.crop(bbox)

            # Extract the table
            raw_data = pg.extract_table(
                table_settings=dict(
                    vertical_strategy="text",
                    horizontal_strategy="text",
                    keep_blank_chars=True,
                )
            )

            # Format into a data frame and return
            data = [
                [unicodedata.normalize("NFKD", w) for w in words] for words in raw_data
            ]
            return pd.DataFrame(data)

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform the raw parsing data into a clean data frame."""

        # Apply each of the transformations
        data = (
            data.pipe(convert_to_floats, usecols=data.columns[1:])
            .fillna(0)
            .rename(columns={0: "category"})
            .assign(
                category=lambda df: df.category.apply(
                    lambda x: "_".join(
                        remove_unwanted_chars(
                            remove_parentheses(x.replace("&", "and")).lower(),
                            "‐",
                            ",",
                            ".",
                            "/",
                        ).split()
                    )
                )
            )
            .reset_index(drop=True)
        )

        # Melt and return
        return data.melt(
            id_vars="category", var_name="fiscal_month", value_name="amount"
        )

    def load(self, data) -> None:
        """Load the data."""

        # Get the path
        dirname = self.get_data_directory("processed")
        tag = str(self.fiscal_year)[2:]
        path = dirname / f"FY{tag}-Q{self.quarter}.csv"

        # Load
        super()._load_csv_data(data, path)
