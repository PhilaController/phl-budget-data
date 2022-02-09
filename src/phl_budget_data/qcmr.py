import numpy as np
import pandas as pd

from .etl import qcmr
from .etl.utils.misc import fiscal_year_quarter_from_path

__all__ = [
    "load_cash_reports",
    "load_department_obligations",
    "load_fulltime_positions",
    "load_personal_services_summary",
]


def _load_processed_results(cls):
    """Internal helper function for loading processed results."""

    # Get the files
    dirname = cls.get_data_directory("processed")
    files = sorted(dirname.glob("*.csv"), reverse=True)

    # Loop over each file
    for f in files:

        # Get fiscal year and quarter
        fiscal_year, quarter = fiscal_year_quarter_from_path(f)

        # Yield
        yield f, fiscal_year, quarter


def _load_department_reports(cls):
    """Internal function to load department-based QCMR reports."""

    all_df = []
    for f, fiscal_year, quarter in _load_processed_results(cls):

        # Get fiscal year and quarter
        fiscal_year, quarter = fiscal_year_quarter_from_path(f)

        # Load
        df = pd.read_csv(f, dtype={"dept_code": str})

        # Add report fiscal year and quarter
        df["report_quarter"] = quarter
        df["report_fiscal_year"] = fiscal_year

        # Save
        all_df.append(df)

    # Combine them!
    out = pd.concat(all_df, ignore_index=True)

    # Make into a date
    out["as_of_date"] = pd.to_datetime(out["as_of_date"])

    # Drop duplicates
    out = out.drop_duplicates(
        subset=["dept_name", "fiscal_year", "variable", "time_period"]
    )

    return out.sort_values(
        ["report_fiscal_year", "report_quarter"], ascending=False
    ).reset_index(drop=True)


def load_personal_services_summary() -> pd.DataFrame:
    """
    Load data from the QCMR Personal Services Summary.

    Notes
    -----
    See raw PDF files in data/raw/qcmr/personal-services/ folder.
    """
    return _load_department_reports(qcmr.PersonalServices)


def load_fulltime_positions() -> pd.DataFrame:
    """
    Load data from the QCMR Full-Time Position Report.

    Notes
    -----
    See raw PDF files in the "data/raw/qcmr/positions/" folder.
    """

    all_df = []
    for f, fiscal_year, quarter in _load_processed_results(qcmr.FullTimePositions):

        # Read the CSV file
        df = pd.read_csv(f, dtype={"dept_code": str})

        # Remove duplicate actuals
        if quarter != 4:
            df = df.query(f"fiscal_year == {fiscal_year}").copy()
        else:
            sel = (df["kind"] == "Actual") & (df["fiscal_year"] == fiscal_year)
            df = df.loc[~sel].copy()

        df["report_quarter"] = quarter
        df["report_fiscal_year"] = fiscal_year
        all_df.append(df)

    # Combine them!
    out = pd.concat(all_df, ignore_index=True)

    return out.sort_values(
        ["report_fiscal_year", "report_quarter"], ascending=False
    ).reset_index(drop=True)


def load_department_obligations() -> pd.DataFrame:
    """
    Load data from the QCMR department obligation reports.

    Notes
    -----
    See raw PDF files in the "data/raw/qcmr/obligations/" folder.
    """
    return _load_department_reports(qcmr.DepartmentObligations)


def load_cash_reports(kind: str) -> pd.DataFrame:
    """
    Load data from the QCMR cash reports.

    Parameters
    ----------
    kind : str
        the kind of data to load, one of "fund-balances",
        "net-cash-flow", "revenue", or "spending"


    Notes
    -----
    See raw PDF files in the "data/raw/qcmr/cash/" folder.
    """
    # Check input
    kinds = ["fund-balances", "net-cash-flow", "revenue", "spending"]
    if kind not in kinds:
        raise ValueError(f"'kind' should be one of: {kinds}")

    classes = {
        "fund-balances": qcmr.CashReportFundBalances,
        "net-cash-flow": qcmr.CashReportNetCashFlow,
        "revenue": qcmr.CashReportRevenue,
        "spending": qcmr.CashReportSpending,
    }
    cls = classes[kind]

    # Formatting
    formatting = {
        "spending": {
            "payroll": "Payroll",
            "employee_benefits": "Employee Benefits",
            "pension": "Pension",
            "purchases_of_services": "Contracts / Leases",
            "materials_equipment": "Materials / Equipment",
            "contributions_indemnities": "Contributions / Indemnities",
            "advances_misc_payments": "Advances / Labor Obligations",
            "debt_service_long": "Long-Term Debt Service",
            "debt_service_short": "Short-Term Debt Service",
            "current_year_appropriation": "Current Year Appropriation",
            "total_disbursements": "Total Disbursements",
            "prior_year_encumbrances": "Prior Year Encumbrances",
            "prior_year_vouchers_payable": "Prior Year Vouchers Payable",
            "interfund_charges": "Interfund Charges",
        },
        "revenue": {
            "real_estate_tax": "Real Estate Tax",
            "wage_earnings_net_profits": "Wage, Earnings, Net Profits",
            "total_wage_earnings_net_profits": "Wage, Earnings, Net Profits",
            "realty_transfer_tax": "Realty Transfer Tax",
            "sales_tax": "Sales Tax",
            "business_income_and_receipts_tax": "BIRT",
            "beverage_tax": "Beverage Tax",
            "total_pica_other_governments": "PICA Other Governments",
            "total_other_governments": "Other Governments",
            "total_cash_receipts": "Total Cash Receipts",
            "locally_generated_nontax": "Locally Generated Non-Tax",
            "other_taxes": "Other Taxes",
            "collection_of_prior_year_revenue": "Prior Year Revenue",
            "interfund_transfers": "Interfund Transfers",
            "other_fund_balance_adjustments": "Other Adjustments",
            "total_current_revenue": "Total Current Revenue",
        },
        "fund-balances": {
            "general": "General Fund",
            "community_development": "Community Development",
            "hospital_assessment_fund": "Hospital Assessment Fund",
            "housing_trust_fund": "Housing Trust Fund",
            "budget_stabilization_fund": "Budget Stabilization Fund",
            "other_funds": "Other Funds",
            "total_operating_funds": "Total Operating Funds",
            "capital_improvement": "Capital Improvement",
            "industrial_and_commercial_dev": "Industrial and Commercial Development",
            "total_capital_funds": "Total Capital Funds",
            "grants_revenue": "Grants Fund",
            "total_fund_equity": "Consolidated Cash",
            "vehicle_rental_tax": "Vehicle Rental Tax",
        },
        "net-cash-flow": {
            "tran": "TRAN",
            "closing_balance": "Closing Balance",
            "excess_of_receipts_over_disbursements": "Receipts - Disbursements",
            "opening_balance": "Opening Balance",
        },
    }

    # Loop over all files
    out = []
    for f, fiscal_year, quarter in _load_processed_results(cls):

        # Load the CSV data
        df = pd.read_csv(f)

        # Drop month = 13 (total)
        df = df.query("fiscal_month != 13")

        df = df.assign(
            fiscal_year=fiscal_year,
            quarter=quarter,
            month=lambda df: np.where(
                df.fiscal_month < 7, df.fiscal_month + 6, df.fiscal_month - 6
            ),
        )

        categories = df["category"].drop_duplicates()
        missing = ~categories.isin(formatting[kind])
        if missing.sum():
            missing = categories.loc[missing]
            raise ValueError(f"Missing category replacements: {missing.tolist()}")
        df["category"] = df["category"].replace(formatting[kind])

        out.append(df)

    return pd.concat(out, ignore_index=True).sort_values(
        ["fiscal_year", "quarter"], ascending=False, ignore_index=True
    )
