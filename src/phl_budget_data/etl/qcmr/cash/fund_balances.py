import pandas as pd

from ...utils.pdf import find_phrases
from .core import CashFlowForecast


class CashReportFundBalances(CashFlowForecast):
    """A class for the fund balances in the cash flow forecast."""

    report_type = "fund-balances"

    def extract(self) -> pd.DataFrame:
        """Internal function to parse the contents of the PDF."""

        # Get the bounding box
        upper_left = find_phrases(self.words, "General")
        bottom_left = find_phrases(self.words, "TOTAL FUND EQUITY")

        bbox = [
            upper_left[0].x0,
            upper_left[0].top,
            None,
            bottom_left[0].bottom,
        ]
        return self._extract_from_page(pg_num=1, bbox=bbox)

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
            assert diff.all() <= ALLOWED_DIFF

        return True
