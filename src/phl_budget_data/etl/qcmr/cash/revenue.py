import pandas as pd

from ...utils.pdf import find_phrases
from .core import CashFlowForecast


class CashReportRevenue(CashFlowForecast):
    """A class for the revenue cash flow forecast."""

    report_type = "revenue"

    def extract(self) -> pd.DataFrame:
        """Internal function to parse the contents of the PDF."""

        # Get the bounding box
        upper_left = find_phrases(self.words, "REVENUES")
        bottom_left = find_phrases(self.words, "TOTAL CASH RECEIPTS")
        upper_right = find_phrases(self.words, "Accrued")

        bbox = [
            upper_left[0].x0,
            upper_left[0].bottom,
            upper_right[0].x0,
            bottom_left[0].bottom,
        ]

        return self._extract_from_page(pg_num=0, bbox=bbox)

    def validate(self, data):
        """Validate the input data."""

        # Make sure we have 13 months worth of data
        # 12 months + 1 for the total
        assert (data["category"].value_counts() == 13).all()

        def compare_totals(X, Y):
            # The difference between the two
            diff = (X - Y).abs()

            # Check
            ALLOWED_DIFF = 0.3
            assert diff.all() <= ALLOWED_DIFF

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
                data.query("fiscal_month != 13 and category in  @cats_to_sum")
                .groupby("fiscal_month")["amount"]
                .sum()
            )
            Y = data.query(f"category == '{total_column}'").set_index("fiscal_month")[
                "amount"
            ]
            compare_totals(X, Y)

        return True
