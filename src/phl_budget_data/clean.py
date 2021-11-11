import calendar

import pandas as pd

from . import DATA_DIR
from .etl.collections import *
from .etl.qcmr import (
    CashReportFundBalances,
    CashReportNetCashFlow,
    CashReportRevenue,
    CashReportSpending,
)
from .etl.utils.misc import fiscal_from_calendar_year

__all__ = [
    "load_rtt_collections_by_sector",
    "load_birt_collections_by_sector",
    "load_sales_collections_by_sector",
    "load_wage_collections_by_sector",
    "load_city_collections",
    "load_school_collections",
    "load_qcmr_cash_reports",
]


def load_rtt_collections_by_sector() -> pd.DataFrame:
    """Load monthly RTT tax collections by sector."""

    # Get the path to the files to load
    dirname = RTTCollectionsBySector.get_data_directory("processed")
    files = dirname.glob("*.csv")

    out = []
    for f in files:

        # Get month/year
        year, month = map(int, f.stem.split("_"))
        month_name = calendar.month_abbr[month].lower()

        # Determine the fiscal year and tags
        fiscal_year = fiscal_from_calendar_year(month, year)

        # Load the data
        df = pd.read_csv(f)
        df = df.query("sector != 'Total'")

        # Trim to the columns we want
        X = df[["total", "num_records", "sector", "parent_sector"]]

        # Melt the data
        X = X.assign(
            month_name=month_name,
            month=month,
            fiscal_month=((month - 7) % 12 + 1),
            year=year,
            fiscal_year=fiscal_year,
            total=lambda df: df.total.fillna(0),
        )

        # Save
        out.append(X)

    # Combine multiple months
    out = pd.concat(out, ignore_index=True)
    out["date"] = pd.to_datetime(
        out["month"].astype(str) + "/" + out["year"].astype(str)
    )

    return out.sort_values("date", ascending=False).reset_index(drop=True)


def load_sales_collections_by_sector() -> pd.DataFrame:
    """Load annual sales tax collections by sector."""

    # Get the path to the files to load
    dirname = SalesCollectionsBySector.get_data_directory("processed")
    files = dirname.glob("*.csv")

    out = []
    for f in files:

        # Get fiscal year
        fiscal_year = int(f"20{f.stem[2:]}")

        # Load the data
        df = (
            pd.read_csv(f)[["total", "sector", "parent_sector"]]
            .query("sector != 'Subtotal'")
            .assign(
                fiscal_year=fiscal_year,
            )
        )

        # Save
        out.append(df)

    # Combine multiple months
    out = pd.concat(out, ignore_index=True)

    return out.sort_values(
        ["fiscal_year", "parent_sector", "sector"], ascending=True
    ).reset_index(drop=True)


def load_birt_collections_by_sector() -> pd.DataFrame:
    """Load annual BIRT collections by sector."""

    # Get the path to the files to load
    dirname = BIRTCollectionsBySector.get_data_directory("processed")
    files = dirname.glob("*.csv")

    out = []
    for f in files:
        out.append(pd.read_csv(f))

    # Combine multiple months
    out = pd.concat(out, ignore_index=True)

    return out.sort_values(
        ["tax_year", "parent_sector", "sector"], ascending=True
    ).reset_index(drop=True)


def load_wage_collections_by_sector() -> pd.DataFrame:
    """Load monthly wage tax collections by sector."""

    # Get the path to the files to load
    dirname = WageCollectionsBySector.get_data_directory("processed")
    files = dirname.glob("*.csv")

    out = []
    for f in files:

        # Get month/year
        year, month = map(int, f.stem.split("-"))
        month_name = calendar.month_abbr[month].lower()

        # Determine the fiscal year and tags
        fiscal_year = fiscal_from_calendar_year(month, year)
        this_FY = str(fiscal_year)[2:]
        last_FY = str(fiscal_year - 1)[2:]

        # Load the data and trim to "total"
        df = pd.read_csv(f)

        # Trim to the columns we want
        X = df[[f"{month_name}_{year}", "sector", "parent_sector"]]

        # Melt the data
        X = (
            X.melt(
                id_vars=["sector", "parent_sector"],
                value_name="total",
            )
            .assign(
                month_name=month_name,
                month=month,
                fiscal_month=((month - 7) % 12 + 1),
                year=year,
                fiscal_year=fiscal_year,
                total=lambda df: df.total.fillna(0),
            )
            .drop(labels=["variable"], axis=1)
        )

        # Save
        out.append(X)

    # Combine multiple months
    out = pd.concat(out, ignore_index=True)
    out["date"] = pd.to_datetime(
        out["month"].astype(str) + "/" + out["year"].astype(str)
    )

    return out.sort_values("date", ascending=False).reset_index(drop=True)


def _load_monthly_collections(files, total_only=False):
    """Internal function to load monthly collections data."""

    out = []

    # IMPORTANT: loop over files in descending order
    for f in sorted(files, reverse=True):

        # Get month/year
        year, month, *_ = f.stem.split("-")
        year = int(year)
        month = int(month)
        month_name = calendar.month_abbr[month].lower()

        # Determine the fiscal year and tags
        fiscal_year = fiscal_from_calendar_year(month, year)
        this_FY = str(fiscal_year)[2:]
        last_FY = str(fiscal_year - 1)[2:]

        # Load the data
        df = pd.read_csv(f)
        if total_only:
            df = df.query("kind == 'total'")

        # Keep the kind column?
        keep_kind = "kind" in df.columns and df["kind"].nunique() > 1

        # Column names for this month
        a = f"{month_name}_fy{this_FY}"
        b = f"{month_name}_fy{last_FY}"

        # Trim to the columns we want
        cols = ["name", a, b]
        if keep_kind:
            cols.append("kind")
        X = df[cols].rename(columns=dict(zip([a, b], [fiscal_year, fiscal_year - 1])))

        id_vars = ["name"]
        if keep_kind:
            id_vars.append("kind")

        # Melt the data
        X = X.melt(id_vars=id_vars, var_name="fiscal_year", value_name="total").assign(
            month_name=month_name,
            month=month,
            fiscal_month=((month - 7) % 12 + 1),
            year=lambda df: df.apply(
                lambda r: r["fiscal_year"] if r["month"] < 7 else r["fiscal_year"] - 1,
                axis=1,
            ),
        )
        X = X.fillna(0)

        # Save
        out.append(X)

    # Combine multiple months
    out = pd.concat(out, ignore_index=True)
    out["date"] = pd.to_datetime(
        out["month"].astype(str) + "/" + out["year"].astype(str)
    )

    # IMPORTANT: drop duplicates, keeping first
    # This keeps latest data, if it is revised
    subset = ["name", "month", "year"]
    if keep_kind:
        subset.append("kind")
    out = out.drop_duplicates(subset=subset, keep="first")

    return out.sort_values("date", ascending=False).reset_index(drop=True)


def load_city_tax_collections() -> pd.DataFrame:
    """Tax collections."""

    # Get the path to the files to load
    dirname = CityTaxCollections.get_data_directory("processed")
    files = dirname.glob(f"*-tax.csv")

    return _load_monthly_collections(files, total_only=False)


def load_city_collections() -> pd.DataFrame:
    """
    Load monthly collections for the City of Philadelphia. This includes tax, non-tax,
    and other government collections.
    """

    labels = ["Tax", "Non-Tax", "Other Govt"]
    tags = ["tax", "nontax", "other-govts"]
    classes = [CityTaxCollections, CityNonTaxCollections, CityOtherGovtsCollections]

    out = []
    for cls, label, tag in zip(classes, labels, tags):

        # Get the path to the files to load
        dirname = cls.get_data_directory("processed")
        files = dirname.glob(f"*-{tag}.csv")

        # Load
        out.append(
            _load_monthly_collections(files, total_only=(tag == "tax")).assign(
                kind=label
            )
        )

    return pd.concat(out, ignore_index=True)


def load_school_collections() -> pd.DataFrame:
    """Load monthly tax collections for the School District."""

    # Get the path to the files to load
    dirname = SchoolTaxCollections.get_data_directory("processed")
    files = dirname.glob("*-tax.csv")

    return _load_monthly_collections(files, total_only=True)


def load_qcmr_cash_reports(kind) -> pd.DataFrame:
    """Load data from the QCMR cash reports."""

    kinds = ["fund-balances", "net-cash-flow", "revenue", "spending"]
    if kind not in kinds:
        raise ValueError(f"'kind' should be one of: {kinds}")

    classes = {
        "fund-balances": CashReportFundBalances,
        "net-cash-flow": CashReportNetCashFlow,
        "revenue": CashReportRevenue,
        "spending": CashReportSpending,
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

    # Get the path to the files to load
    dirname = cls.get_data_directory("processed")
    files = dirname.glob("*.csv")

    out = []
    for f in files:

        # Get fiscal year and quarter
        fiscal_year, quarter = f.stem.split("-")
        fiscal_year = int(f"20{fiscal_year[2:]}")
        quarter = int(quarter[1:])

        # Load the data
        df = pd.read_csv(f)

        # Drop month = 13 (total)
        df = df.query("fiscal_month != 13")

        df = df.assign(
            fiscal_year=fiscal_year,
            quarter=quarter,
            month=lambda df: (df.fiscal_month + 6) % 12 + 1,
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
