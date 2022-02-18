"""PHL Budget Data."""

from importlib.metadata import version
from pathlib import Path

__version__ = version(__package__)

DATA_DIR = Path(__file__).parent.absolute() / "data"
ETL_DATA_DIR = DATA_DIR / "etl"

# ETL install version?
ETL_VERSION = False
# try:
#     import selenium

#     ETL_VERSION = True
# except:
#     pass
