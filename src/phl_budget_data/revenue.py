import calendar

import pandas as pd

from .utils import ETL_VERSION, load_from_cache

if ETL_VERSION:
    from .etl import collections
    from .etl.utils.misc import fiscal_from_calendar_year

__all__ = [
    "load_birt_collections_by_sector",
    "load_sales_collections_by_sector",
    "load_wage_collections_by_sector",
    "load_city_collections",
    "load_city_tax_collections",
    "load_school_collections",
]


@load_from_cache
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


@load_from_cache
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


@load_from_cache
def load_wage_collections_by_sector() -> pd.DataFrame:
    """Load quarterly wage tax collections by sector."""

    # Get the path to the files to load
    dirname = collections.WageCollectionsBySector.get_data_directory("processed")
    files = dirname.glob("*.csv")

    # Month set
    month_set = "|".join([calendar.month_abbr[i].lower() for i in range(1, 13)])

    # Fiscal quarters
    fiscal_quarters = {
        "jul": 1,
        "aug": 1,
        "sep": 1,
        "oct": 2,
        "nov": 2,
        "dec": 2,
        "jan": 3,
        "feb": 3,
        "mar": 3,
        "apr": 4,
        "may": 4,
        "jun": 4,
    }

    out = []
    for f in files:

        # Get month/year
        year, month = map(int, f.stem.split("-"))
        month_name = calendar.month_abbr[month].lower()

        # Determine the fiscal year and tags
        fiscal_year = fiscal_from_calendar_year(month, year)

        # Load the data and trim to "total"
        df = pd.read_csv(f)

        # Check for quarterly data
        # Example: "jan_to_mar_2022"
        value_data = df.filter(regex=f"({month_set})_to_({month_set})_{year}$", axis=1)

        # Monthly data?
        if not len(value_data.columns):
            value_data = df.filter(regex=f"({month_set})_{year}$", axis=1)

        # Join the data with id columns
        X = df[["sector", "parent_sector"]].join(value_data)

        # Melt the data
        X = (
            X.melt(
                id_vars=["sector", "parent_sector"],
                value_name="total",
            )
            .assign(
                fiscal_quarter=fiscal_quarters[month_name],
                year=year,
                fiscal_year=fiscal_year,
                total=lambda df: df.total.fillna(0),
            )
            .drop(labels=["variable"], axis=1)
        )

        # Save
        out.append(X)

    # Combine into a single dataframe
    out = pd.concat(out, ignore_index=True)

    # Aggregate by quarter
    out = out.groupby(
        ["sector", "parent_sector", "year", "fiscal_year"] + ["fiscal_quarter"],
        as_index=False,
        dropna=False,
    )["total"].sum()

    # Map quarter to month
    out["month_start"] = out["fiscal_quarter"].replace(
        {1: "07", 2: "09", 3: "01", 4: "04"}
    )

    # Add date
    out["date"] = pd.to_datetime(
        out.apply(lambda r: f"{r['year']}-{r['month_start']}", axis=1)
    )

    return out.sort_values("date", ascending=False, ignore_index=True).drop(
        columns=["month_start"]
    )


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


@load_from_cache
def load_city_tax_collections() -> pd.DataFrame:
    """Load monthly City tax collections."""

    # Get the path to the files to load
    dirname = collections.CityTaxCollections.get_data_directory("processed")
    files = dirname.glob(f"*-tax.csv")

    return _load_monthly_collections(files, total_only=False)


@load_from_cache
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


@load_from_cache
def load_school_collections() -> pd.DataFrame:
    """Load monthly tax collections for the School District."""

    # Get the path to the files to load
    dirname = collections.SchoolTaxCollections.get_data_directory("processed")
    files = dirname.glob("*-tax.csv")

    return _load_monthly_collections(files, total_only=True)
