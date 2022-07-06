"""Load processed collections data from the data cache."""

import pandas as pd

from . import DATA_DIR

CACHE_DIR = DATA_DIR / "processed" / "spending"


__all__ = [
    "load_birt_collections_by_sector",
    "load_sales_collections_by_sector",
    "load_wage_collections_by_sector",
    "load_city_collections",
    "load_city_tax_collections",
    "load_school_collections",
]


def load_sales_collections_by_sector() -> pd.DataFrame:
    """Load annual sales tax collections by sector."""
    return pd.read_csv(CACHE_DIR / "sales-collections-by-sector.csv")


def load_birt_collections_by_sector() -> pd.DataFrame:
    """Load annual BIRT collections by sector."""
    return pd.read_csv(CACHE_DIR / "birt-collections-by-sector.csv")


def load_wage_collections_by_sector() -> pd.DataFrame:
    """Load quarterly wage tax collections by sector."""
    return pd.read_csv(CACHE_DIR / "wage-collections-by-sector.csv")


def load_city_tax_collections() -> pd.DataFrame:
    """Load monthly City tax collections."""
    return pd.read_csv(CACHE_DIR / "city-tax-collections.csv")


def load_city_collections() -> pd.DataFrame:
    """
    Load monthly collections for the City of Philadelphia. This includes tax, non-tax,
    and other government collections.
    """
    return pd.read_csv(CACHE_DIR / "city-collections.csv")


def load_school_collections() -> pd.DataFrame:
    """Load monthly tax collections for the School District."""
    return pd.read_csv(CACHE_DIR / "school-collections.csv")
