"""Base class for parsing the Cash Flow Forecast from the QCMR."""
from typing import ClassVar

import pandas as pd

from ...utils.transformations import convert_to_floats, fix_decimals, replace_commas
from ..base import ETLPipelineQCMR


class CashFlowForecast(ETLPipelineQCMR):
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

    dtype: ClassVar[str] = "cash"
    report_type: ClassVar[str]

    @classmethod
    def get_data_directory(cls, kind: str) -> str:
        """Internal function to get the file path.

        Parameters
        ----------
        kind : {'raw', 'interim', 'processed'}
            type of data to load
        """
        assert kind in ["raw", "interim", "processed"]

        dirname = super().get_data_directory(kind)
        if kind == "processed":
            dirname = dirname / cls.report_type

        return dirname

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

        # Melt and return
        return data.melt(
            id_vars="category", var_name="fiscal_month", value_name="amount"
        ).assign(fiscal_month=lambda df: df.fiscal_month.astype(int))
