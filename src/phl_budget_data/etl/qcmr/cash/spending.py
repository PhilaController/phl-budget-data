"""Spending data from the cash report."""

from typing import ClassVar

import pandas as pd
from loguru import logger
from pydantic import BaseModel, Field, validator

from ...core import validate_data_schema
from ...utils.misc import get_index_label
from .core import CASH_DATA_TYPE, CashFlowForecast

# Row headers
CATEGORIES = [
    "payroll",
    "employee_benefits",
    "pension",
    "purchases_of_services",
    "materials_equipment",
    "contributions_indemnities",
    "debt_service_short",
    "debt_service_long",
    "interfund_charges",
    "advances_misc_payments",
    "current_year_appropriation",
    "prior_year_encumbrances",
    "prior_year_vouchers_payable",
    "total_disbursements",
]


class CashSpendingSchema(BaseModel):
    """Schema for the General Fund cash spending data from the QCMR."""

    amount: float = Field(title="Cash Amount", description="The cash amount.")
    fiscal_month: int = Field(
        title="Fiscal Month",
        description="The fiscal month, where 1 equals July",
        ge=1,
        le=13,
    )
    category: str = Field(
        title="Category",
        description="The spending category.",
    )

    @validator("category")
    def category_ok(cls, category: str) -> str:
        """Validate the 'category' field."""
        if category not in CATEGORIES:
            raise ValueError(f"'category' should be one of: {', '.join(CATEGORIES)}")

        return category


class CashReportSpending(CashFlowForecast): #type: ignore
    """General Fund cash spending from the QCMR's Cash Flow Forecast."""

    report_dtype: ClassVar[CASH_DATA_TYPE] = "spending"

    def extract(self) -> pd.DataFrame:
        """Extract the contents of the PDF."""

        # Get the Textract output
        df = self._get_textract_output(pg_num=1)

        # Trim to Revenue section
        start = get_index_label(df, "Payro.*l", how="contains")
        stop = get_index_label(df, "TOTAL DISBURSEMENTS")

        # Keep first 14 columns (category + 12 months + total)
        out = df.loc[start:stop, [str(i) for i in range(0, 14)]]

        # Remove empty rows
        return out.dropna(how="all", subset=map(str, range(1, 14)))

    @validate_data_schema(data_schema=CashSpendingSchema)
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform the raw parsing data into a clean data frame."""

        # Check the length
        if len(data) != len(CATEGORIES):
            fy = str(self.fiscal_year)[2:]
            tag = f"FY{fy} Q{self.quarter}"
            raise ValueError(f"Parsing error for spending data in {tag} cash report")

        # Set the categories
        data["0"] = CATEGORIES
        return super().transform(data)

    def validate(self, data: pd.DataFrame) -> bool:
        """Validate the input data."""

        # Make sure we have 13 months worth of data
        # 12 months + 1 for the total
        assert (data["category"].value_counts() == 13).all()

        def compare_totals(X: pd.Series, Y: pd.Series) -> None:
            # The difference between the two
            diff = (X - Y).abs()

            # Check
            ALLOWED_DIFF = 0.301
            if not (diff <= ALLOWED_DIFF).all():
                logger.info(diff)
                assert (diff <= ALLOWED_DIFF).all()

        # Sum over months for each category and compare to parsed total
        X = data.query("fiscal_month != 13").groupby("category")["amount"].sum()
        Y = data.query("fiscal_month == 13").set_index("category")["amount"]

        # Compare
        compare_totals(X, Y)

        # Sum over months and compare to parsed total
        groups = {
            "current_year_appropriation": [
                "payroll",
                "employee_benefits",
                "pension",
                "purchases_of_services",
                "materials_equipment",
                "contributions_indemnities",
                "debt_service_short",
                "debt_service_long",
                "interfund_charges",
                "advances_misc_payments",
            ],
            "total_disbursements": [
                "current_year_appropriation",
                "prior_year_encumbrances",
                "prior_year_vouchers_payable",
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
            compare_totals(X, Y)

        return True
