import pandas as pd
from loguru import logger

from ...utils.transformations import remove_parentheses, remove_unwanted_chars
from .core import CashFlowForecast


class CashReportFundBalances(CashFlowForecast):
    """A class for the fund balances in the cash flow forecast."""

    report_type = "fund-balances"

    def extract(self) -> pd.DataFrame:
        """Extract the contents of the PDF."""

        # Get the Textract output
        df = self._get_textract_output(pg_num=2)

        # Remove first row and empty rows
        return df.dropna(how="all").iloc[1:]

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform the raw parsing data into a clean data frame."""

        categories = [
            "vehicle_rental_tax",
            "community_development",
            "grants_revenue",
            "total_capital_funds",
            "total_fund_equity",
            "industrial_and_commercial_dev",
            "other_funds",
            "capital_improvement",
            "total_operating_funds",
            "general",
            "housing_trust_fund",
            "hospital_assessment_fund",
            "budget_stabilization_fund",
        ]

        # Transform the category
        transform = lambda x: "_".join(
            remove_unwanted_chars(
                remove_parentheses(x.replace("&", "and")).lower(),
                "‐",
                ",",
                ".",
                "/",
            ).split()
        )
        data["0"] = data["0"].apply(transform)

        # Check the length
        if not data["0"].isin(categories).all():
            fy = str(self.fiscal_year)[2:]
            tag = f"FY{fy} Q{self.quarter}"
            raise ValueError(
                f"Parsing error for fund balance data in {tag} cash report"
            )

        # Return
        return super().transform(data)

    def validate(self, data):
        """Validate the input data."""

        # Make sure we have 12 months worth of data
        assert (data["category"].value_counts() == 12).all()

        groups = {
            "total_operating_funds": [
                "general",
                "grants_revenue",
                "community_development",
                "vehicle_rental_tax",
                "hospital_assessment_fund",
                "housing_trust_fund",
                "budget_stabilization_fund",
                "other_funds",
            ],
            "total_capital_funds": [
                "capital_improvement",
                "industrial_and_commercial_dev",
            ],
            "total_fund_equity": ["total_operating_funds", "total_capital_funds"],
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
