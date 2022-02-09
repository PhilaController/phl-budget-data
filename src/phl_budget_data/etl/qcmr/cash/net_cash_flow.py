import pandas as pd
from loguru import logger
from pydantic import BaseModel, Field, validator

from ...core import validate_data_schema
from ...utils.misc import get_index_label
from .core import CashFlowForecast

CATEGORIES = [
    "excess_of_receipts_over_disbursements",
    "opening_balance",
    "tran",
    "closing_balance",
]


class NetCashFlowSchema(BaseModel):
    """Schema for the General Fund cash flow data from the QCMR."""

    amount: float = Field(title="Cash Amount", description="The cash amount.")
    fiscal_month: int = Field(
        title="Fiscal Month",
        description="The fiscal month, where 1 equals July",
        ge=1,
        le=12,
    )
    category: str = Field(
        title="Category",
        description="The net cash flow category.",
    )

    @validator("category")
    def category_ok(cls, category):
        """Validate the 'category' field."""
        if category not in CATEGORIES:
            raise ValueError(f"'category' should be one of: {', '.join(CATEGORIES)}")

        return category


class CashReportNetCashFlow(CashFlowForecast):
    """A class for the General Fund cash flow in the cash flow forecast."""

    report_type = "net-cash-flow"

    def extract(self) -> pd.DataFrame:
        """Extract the contents of the PDF."""

        # Get the Textract output
        df = self._get_textract_output(pg_num=1)

        # Trim to Revenue section
        start = get_index_label(df, "TOTAL DISBURSEMENTS") + 1
        stop = None

        # Keep first 14 columns (category + 12 months + total)
        out = df.loc[start:stop, "0":"12"]

        return out.dropna(how="all", subset=map(str, range(1, 13)))

    @validate_data_schema(data_schema=NetCashFlowSchema)
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform the raw parsing data into a clean data frame."""

        # Check the length
        if len(data) != len(CATEGORIES):
            fy = str(self.fiscal_year)[2:]
            tag = f"FY{fy} Q{self.quarter}"
            raise ValueError(
                f"Parsing error for net cash flow data in {tag} cash report"
            )

        # Set the categories
        data["0"] = CATEGORIES
        return super().transform(data)

    def validate(self, data):
        """Validate the input data."""

        # Make sure we have 12 months worth of data
        assert (data["category"].value_counts() == 12).all()

        groups = {
            "closing_balance": [
                "excess_of_receipts_over_disbursements",
                "opening_balance",
                "tran",
            ],
        }

        # Sum up categories and compare to parsed totals
        for total_column, cats_to_sum in groups.items():

            X = (
                data.query("category in  @cats_to_sum")
                .groupby("fiscal_month")["amount"]
                .sum()
            )
            Y = data.query(f"category == '{total_column}'").set_index("fiscal_month")[
                "amount"
            ]
            diff = (X - Y).abs()

            # Check
            ALLOWED_DIFF = 0.3
            if not (diff <= ALLOWED_DIFF).all():
                logger.info(diff)
                assert (diff <= ALLOWED_DIFF).all()

        return True
