"""Base class for parsing the Cash Flow Forecast from the QCMR."""

from dataclasses import dataclass
from typing import ClassVar

import pandas as pd

from ... import DATA_DIR
from ...etl import ETLPipeline
from ...utils.aws import parse_pdf_with_textract
from ...utils.transformations import convert_to_floats, fix_decimals, replace_commas


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

    @classmethod
    def get_data_directory(cls, kind: str) -> str:
        """Internal function to get the file path.

        Parameters
        ----------
        kind : {'raw', 'interim', 'processed'}
            type of data to load
        """
        assert kind in ["raw", "interim", "processed"]

        if kind in ["raw", "interim"]:
            return DATA_DIR / kind / "qcmr" / "cash"
        else:
            return DATA_DIR / kind / "qcmr" / "cash" / f"{cls.report_type}"

    def _get_textract_output(self, pg_num):
        """Use AWS Textract to extract the contents of the PDF."""

        # Get the file name
        interim_dir = self.get_data_directory("interim")
        get_filename = lambda i: interim_dir / f"{self.path.stem}-pg-{i}.csv"

        # The requested file name
        filename = get_filename(pg_num)

        # We need to parse
        if not filename.exists():

            # Initialize the output folder if we need to
            if not interim_dir.is_dir():
                interim_dir.mkdir(parents=True)

            # Extract with textract
            parsing_results = parse_pdf_with_textract(
                self.path, bucket_name="phl-budget-data"
            )

            # Save each page result
            for i, df in parsing_results:
                df.to_csv(get_filename(i), index=False)

        # Return the result
        return pd.read_csv(filename)

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform the raw parsing data into a clean data frame."""

        # Apply each of the transformations
        data = (
            data.pipe(replace_commas, usecols=data.columns[1:])
            .pipe(fix_decimals, usecols=data.columns[1:])
            .pipe(convert_to_floats, usecols=data.columns[1:])
            .fillna(0)
            .rename(columns={"0": "category"})
            .reset_index(drop=True)
        )
        print(data)

        # Melt and return
        return data.melt(
            id_vars="category", var_name="fiscal_month", value_name="amount"
        ).assign(fiscal_month=lambda df: df.fiscal_month.astype(int))

    def load(self, data) -> None:
        """Load the data."""

        # Get the path
        dirname = self.get_data_directory("processed")
        tag = str(self.fiscal_year)[2:]
        path = dirname / f"FY{tag}-Q{self.quarter}.csv"

        # Load
        super()._load_csv_data(data, path)
