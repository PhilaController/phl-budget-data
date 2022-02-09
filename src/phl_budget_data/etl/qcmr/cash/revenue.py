import pandas as pd
from loguru import logger
from pydantic import BaseModel, Field, validator

from ...core import validate_data_schema
from ...utils.misc import get_index_label
from .core import CashFlowForecast

CATEGORIES = [
    "real_estate_tax",
    "total_wage_earnings_net_profits",
    "realty_transfer_tax",
    "sales_tax",
    "business_income_and_receipts_tax",
    "beverage_tax",
    "other_taxes",
    "locally_generated_nontax",
    "total_other_governments",
    "total_pica_other_governments",
    "interfund_transfers",
    "total_current_revenue",
    "collection_of_prior_year_revenue",
    "other_fund_balance_adjustments",
    "total_cash_receipts",
]


class CashRevenueSchema(BaseModel):
    """Schema for the General Fund cash revenue data from the QCMR."""

    amount: float = Field(title="Cash Amount", description="The cash amount.")
    fiscal_month: int = Field(
        title="Fiscal Month",
        description="The fiscal month, where 1 equals July",
        ge=1,
        le=13,
    )
    category: str = Field(
        title="Category",
        description="The revenue category.",
    )

    @validator("category")
    def category_ok(cls, category):
        """Validate the 'category' field."""
        if category not in CATEGORIES:
            raise ValueError(f"'category' should be one of: {', '.join(CATEGORIES)}")

        return category


class CashReportRevenue(CashFlowForecast):
    """A class for the revenue cash flow forecast."""

    report_type = "revenue"

    def extract(self) -> pd.DataFrame:
        """Extract the contents of the PDF."""

        # Get the Textract output
        df = self._get_textract_output(pg_num=1)

        # Trim to Revenue section
        start = get_index_label(df, "REVENUES")
        stop = get_index_label(df, "TOTAL CASH RECEIPTS", how="contains")

        if df.loc[start, "1":"13"].isnull().all():
            start += 1

        # Keep first 14 columns (category + 12 months + total)
        out = df.iloc[1:].loc[start:stop, "0":"13"]

        # Remove empty rows
        return out.dropna(how="all")

    @validate_data_schema(data_schema=CashRevenueSchema)
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform the raw parsing data into a clean data frame."""

        # Remove soda tax
        categories = [c for c in CATEGORIES]
        if self.fiscal_year < 2017:
            categories.pop(categories.index("beverage_tax"))

        # Remove extra line
        sel = data["0"].str.contains("Non-re") | data["0"].str.contains("Non-bu")
        data = data[~sel].copy()

        # Try to add adjustments
        tag = "Other fund balance adjustments"
        if (
            self.fiscal_year >= 2011
            and not data["0"].isin(["Other fund balance adjustments"]).any()
        ):

            # Bump the last one
            last = data.iloc[-1]
            data.loc[last.name + 1, :] = last.values

            # Add the empty adjustments
            data.loc[last.name, "0"] = "Other fund balance adjustments"
            data.loc[last.name, "1":] = "0"

            # Sort it
            data = data.sort_index()

        # Check the length
        if len(data) != len(categories):
            fy = str(self.fiscal_year)[2:]
            tag = f"FY{fy} Q{self.quarter}"
            print(data)
            raise ValueError(f"Parsing error for revenue data in {tag} cash report")

        # Set the categories
        data["0"] = categories
        return super().transform(data)

    def validate(self, data):
        """Validate the input data."""

        # Make sure we have 13 months worth of data
        # 12 months + 1 for the total
        assert (data["category"].value_counts() == 13).all()

        def compare_totals(X, Y):
            # The difference between the two
            diff = (X - Y).abs()

            # Check
            ALLOWED_DIFF = 0.401
            if not (diff <= ALLOWED_DIFF).all():
                logger.info(diff)
                assert (diff <= ALLOWED_DIFF).all()

        # Sum over months for each category and compare to parsed total
        X = data.query("fiscal_month != 13").groupby("category")["amount"].sum()
        Y = data.query("fiscal_month == 13").set_index("category")["amount"]

        # Compare
        compare_totals(X, Y)

        groups = {
            "total_current_revenue": [
                "real_estate_tax",
                "total_wage_earnings_net_profits",
                "realty_transfer_tax",
                "sales_tax",
                "business_income_and_receipts_tax",
                "beverage_tax",
                "other_taxes",
                "locally_generated_nontax",
                "total_other_governments",
                "total_pica_other_governments",
                "interfund_transfers",
            ],
            "total_cash_receipts": [
                "total_current_revenue",
                "collection_of_prior_year_revenue",
                "other_fund_balance_adjustments",
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
