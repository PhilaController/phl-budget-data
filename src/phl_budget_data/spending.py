"""Load processed spending data from the data cache."""

from typing import Literal

import pandas as pd
from pydantic import validate_arguments

from . import DATA_DIR

CACHE_DIR = DATA_DIR / "processed" / "spending"

__all__ = ["load_budgeted_department_spending", "load_actual_department_spending"]


@validate_arguments
def load_budgeted_department_spending(
    kind: Literal["adopted", "proposed"]
) -> pd.DataFrame:
    """
    Load budgeted spending by department and major class.

    Source: Annual Budget-in-Brief documents
    """
    return pd.read_csv(CACHE_DIR / f"budgeted-department-spending-{kind}.csv")


def load_actual_department_spending() -> pd.DataFrame:
    """
    Load actual spending by department and major class.

    Source: Annual Budget-in-Brief documents
    """
    return pd.read_csv(CACHE_DIR / f"actual-department-spending.csv")
