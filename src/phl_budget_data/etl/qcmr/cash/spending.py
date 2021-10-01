import pandas as pd

from ...utils.pdf import find_phrases
from .core import CashFlowForecast


class CashReportSpending(CashFlowForecast):
    """A class for the spending cash flow forecast."""

    report_type = "spending"

    def extract(self) -> pd.DataFrame:
        """Internal function to parse the contents of the PDF."""

        # Get the bounding box
        upper_left = find_phrases(self.words, "EXPENSES AND OBLIGATIONS")
        bottom_left = find_phrases(self.words, "TOTAL DISBURSEMENTS")
        upper_right = find_phrases(self.words, "Vouchers")

        bbox = [
            upper_left[0].x0,
            upper_left[0].bottom,
            upper_right[0].x0,
            bottom_left[0].bottom,
        ]

        # Extract
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

        # Sum over months and compare to parsed total
        groups = {
            "current_year_appropriation": [
                "payroll",
                "employee_benefits",
                "pension",
                "purchase_of_services",
                "materials_equipment",
                "contributions_indemnities",
                "debt_serviceshort_term",
                "debt_servicelong_term",
                "interfund_charges",
                "advances_and_misc_pmts_labor_obligations",
            ],
            "total_disbursements": [
                "current_year_appropriation",
                "prior_yr_expenditures_against_encumbrances",
                "prior_yr_salaries_and_vouchers_payable",
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
