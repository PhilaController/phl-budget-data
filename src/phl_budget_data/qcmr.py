from typing import Literal

import numpy as np
import pandas as pd
from pydantic import validate_arguments

from . import DATA_DIR

# Cash report data type
CASH_DATA_TYPE = Literal["fund-balances", "spending", "revenue", "net-cash-flow"]

# Cache folder
CACHE_DIR = DATA_DIR / "processed" / "qcmr"


__all__ = [
    "load_cash_reports",
    "load_department_obligations",
    "load_fulltime_positions",
    "load_personal_services_summary",
]


def load_personal_services_summary() -> pd.DataFrame:
    """
    Load data from the QCMR Personal Services Summary.

    Notes
    -----
    See raw PDF files in data/raw/qcmr/personal-services/ folder.
    """
    return pd.read_csv(
        CACHE_DIR / "personal-services-summary.csv",
        dtype={"dept_code": str, "dept_major_code": str},
        parse_dates=["as_of_date"],
    )


def load_fulltime_positions() -> pd.DataFrame:
    """
    Load data from the QCMR Full-Time Position Report.

    Notes
    -----
    See raw PDF files in the "data/raw/qcmr/positions/" folder.
    """
    return pd.read_csv(
        CACHE_DIR / "fulltime-positions.csv",
        dtype={"dept_code": str, "dept_major_code": str},
        parse_dates=["as_of_date"],
    )


def load_department_obligations() -> pd.DataFrame:
    """
    Load data from the QCMR department obligation reports.

    Notes
    -----
    See raw PDF files in the "data/raw/qcmr/obligations/" folder.
    """
    return pd.read_csv(
        CACHE_DIR / "department-obligations.csv",
        dtype={"dept_code": str, "dept_major_code": str},
        parse_dates=["as_of_date"],
    )


@validate_arguments
def load_cash_reports(
    kind: CASH_DATA_TYPE
) -> pd.DataFrame:
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
    return pd.read_csv(
        CACHE_DIR / f"cash-reports-{kind}.csv"
    )
