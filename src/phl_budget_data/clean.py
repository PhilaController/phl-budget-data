import calendar

import pandas as pd

from . import DATA_DIR
from .etl.collections import *
from .etl.utils.misc import fiscal_from_calendar_year


def load_sales_collections_by_sector() -> pd.DataFrame:
    """
    Load annual sales tax collections by sector

    Returns
    -------
    data :
        the annual sales collection data
    """
    # Get the path to the files to load
    dirname = SalesCollectionsByIndustry.get_data_directory("processed")
    files = dirname.glob("*.csv")

    out = []
    for f in files:

        # Get fiscal year
        fiscal_year = int(f"20{f.stem[2:]}")

        # Load the data
        df = (
            pd.read_csv(f)[["total", "industry", "parent_industry"]]
            .query("industry != 'Subtotal'")
            .assign(
                fiscal_year=fiscal_year,
            )
        )

        # Save
        out.append(df)

    # Combine multiple months
    out = pd.concat(out, ignore_index=True)

    return out.sort_values(
        ["fiscal_year", "parent_industry", "industry"], ascending=True
    )


def load_wage_collections_by_industry() -> pd.DataFrame:
    """
    Load monthly wage tax collections by industry

    Returns
    -------
    data :
        the monthly wage collection data
    """
    # Get the path to the files to load
    dirname = WageCollectionsByIndustry.get_data_directory("processed")
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
        X = df[[f"{month_name}_{year}", "industry", "parent_industry"]]

        # Melt the data
        X = (
            X.melt(
                id_vars=["industry", "parent_industry"],
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

    return out.sort_values("date", ascending=True)


def _load_monthly_collections(files, total_only=False):
    """Internal function to load monthly collections data."""

    out = []
    for f in files:

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

        # Column names for this month
        a = f"{month_name}_fy{this_FY}"
        b = f"{month_name}_fy{last_FY}"

        # Trim to the columns we want
        X = df[["name", a, b]].rename(
            columns=dict(zip([a, b], [fiscal_year, fiscal_year - 1]))
        )

        # Melt the data
        X = (
            X.melt(id_vars=["name"], var_name="fiscal_year", value_name="total")
            .assign(
                month_name=month_name,
                month=month,
                fiscal_month=((month - 7) % 12 + 1),
                year=year,
            )
            .query(f"fiscal_year == {fiscal_year}")
        )
        X = X.fillna(0)

        # Save
        out.append(X)

    # Combine multiple months
    out = pd.concat(out, ignore_index=True)
    out["date"] = pd.to_datetime(
        out["month"].astype(str) + "/" + out["year"].astype(str)
    )

    return out.sort_values("date", ascending=True)


def load_monthly_other_govt_collections() -> pd.DataFrame:
    """
    Load monthly other govt collections for the City.

    Returns
    -------
    data :
        the monthly tax collection data
    """

    # Get the path to the files to load
    dirname = CityOtherGovtsCollections.get_data_directory("processed")
    files = dirname.glob("*-other-govts.csv")

    return _load_monthly_collections(files)


def load_monthly_nontax_collections() -> pd.DataFrame:
    """
    Load monthly nontax collections for the City.

    Returns
    -------
    data :
        the monthly tax collection data
    """

    # Get the path to the files to load
    dirname = CityNonTaxCollections.get_data_directory("processed")
    files = dirname.glob("*-nontax.csv")

    return _load_monthly_collections(files)


def load_monthly_tax_collections(kind: str) -> pd.DataFrame:
    """
    Load monthly tax collections, for either the City or School District.

    Parameters
    ----------
    kind :
        either 'city' or 'school'

    Returns
    -------
    data :
        the monthly tax collection data
    """

    assert kind in ["city", "school"]

    # Get the path to the files to load
    if kind == "city":
        dirname = CityTaxCollections.get_data_directory("processed")
    elif kind == "school":
        dirname = SchoolTaxCollections.get_data_directory("processed")
    files = dirname.glob("*-tax.csv")

    return _load_monthly_collections(files, total_only=True)
