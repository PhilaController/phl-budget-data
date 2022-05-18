"""Base class for parsing the Cash Flow Forecast from the QCMR."""

from pathlib import Path
from typing import ClassVar, Literal

import pandas as pd

from ...utils import transformations as tr
from ..base import ETL_DATA_FOLDERS, QCMR_DATA_TYPE, ETLPipelineQCMR

# Cash report data type
CASH_DATA_TYPE = Literal["fund-balances", "spending", "revenue", "net-cash-flow"]


class CashFlowForecast(ETLPipelineQCMR):  # type: ignore
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

    dtype: ClassVar[QCMR_DATA_TYPE] = "cash"
    report_dtype: ClassVar[CASH_DATA_TYPE]

    @classmethod
    def get_data_directory(cls, kind: ETL_DATA_FOLDERS) -> Path:
        """Internal function to get the file path."""

        dirname = super().get_data_directory(kind)
        if kind == "processed":
            dirname = dirname / cls.report_dtype

        return dirname

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform the raw parsing data into a clean data frame."""

        # Apply each of the transformations
        data = (
            data.pipe(tr.replace_commas, usecols=data.columns[1:])
            .pipe(tr.fix_decimals, usecols=data.columns[1:])
            .pipe(tr.convert_to_floats, usecols=data.columns[1:])
            .fillna(0)
            .rename(columns={"0": "category"})
            .reset_index(drop=True)
        )

        # Melt and return
        return data.melt(
            id_vars="category", var_name="fiscal_month", value_name="amount"
        ).assign(fiscal_month=lambda df: df.fiscal_month.astype(int))
