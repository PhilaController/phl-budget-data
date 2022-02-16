"""PHL Budget Data."""

from importlib.metadata import version
from pathlib import Path

__version__ = version(__package__)

DATA_DIR = Path(__file__).parent.absolute() / "data"
ETL_DATA_DIR = DATA_DIR / "etl"
