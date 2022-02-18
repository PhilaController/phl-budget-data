"""PHL Budget Data."""

from importlib.metadata import version
from pathlib import Path

__version__ = version(__package__)

DATA_DIR = Path(__file__).parent.absolute() / "data"
ETL_DATA_DIR = DATA_DIR / "etl"

# ETL install version?
try:
    from . import etl

    ETL_VERSION = True
except ImportError:
    ETL_VERSION = False
