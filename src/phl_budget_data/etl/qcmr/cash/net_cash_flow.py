import pandas as pd

from ...utils.pdf import find_phrases
from .core import CashFlowForecast


class CashReportNetCashFlow(CashFlowForecast):
    """A class for the General Fund cash flow in the cash flow forecast."""

    report_type = "net-cash-flow"

    def extract(self) -> pd.DataFrame:
        """Internal function to parse the contents of the PDF."""

        # Get the bounding box
        upper_left = find_phrases(self.words, "TOTAL DISBURSEMENTS")
        bottom_left = find_phrases(self.words, "CLOSING BALANCE")
        upper_right = find_phrases(self.words, "Total")

        bbox = [
            upper_left[0].x0,
            upper_left[0].bottom,
            upper_right[0].x0,
            bottom_left[0].bottom,
        ]

        return self._extract_from_page(pg_num=0, bbox=bbox)

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform the data."""
        return super().transform(data).query("category != 'total_disbursements'")

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
            assert diff.all() <= ALLOWED_DIFF

        return True
