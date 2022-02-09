import calendar

import numpy as np
import pandas as pd

from . import DATA_DIR
from .etl import collections
from .etl.utils.misc import fiscal_from_calendar_year

__all__ = [
    "load_rtt_collections_by_sector",
    "load_birt_collections_by_sector",
    "load_sales_collections_by_sector",
    "load_wage_collections_by_sector",
    "load_city_collections",
    "load_city_tax_collections",
    "load_school_collections",
]


def load_rtt_collections_by_sector() -> pd.DataFrame:
    """Load monthly RTT tax collections by sector."""

    # Get the path to the files to load
    dirname = collections.RTTCollectionsBySector.get_data_directory("processed")
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
    dirname = collections.SalesCollectionsBySector.get_data_directory("processed")
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
    dirname = collections.BIRTCollectionsBySector.get_data_directory("processed")
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
    dirname = collections.WageCollectionsBySector.get_data_directory("processed")
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
    """Load monthly City tax collections."""

    # Get the path to the files to load
    dirname = collections.CityTaxCollections.get_data_directory("processed")
    files = dirname.glob(f"*-tax.csv")

    return _load_monthly_collections(files, total_only=False)


def load_city_collections() -> pd.DataFrame:
    """
    Load monthly collections for the City of Philadelphia. This includes tax, non-tax,
    and other government collections.
    """

    labels = ["Tax", "Non-Tax", "Other Govt"]
    tags = ["tax", "nontax", "other-govts"]
    classes = [
        collections.CityTaxCollections,
        collections.CityNonTaxCollections,
        collections.CityOtherGovtsCollections,
    ]

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
    dirname = collections.SchoolTaxCollections.get_data_directory("processed")
    files = dirname.glob("*-tax.csv")

    return _load_monthly_collections(files, total_only=True)
