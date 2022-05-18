"""Main module for data ETL (development version only)."""

from typing import Literal

from .. import DATA_DIR

ETL_DATA_DIR = DATA_DIR / "etl"
ETL_DATA_FOLDERS = Literal["raw", "processed", "interim"]
