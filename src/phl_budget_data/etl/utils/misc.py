from __future__ import annotations

"""Miscellaneous utility functions."""

import re
from pathlib import Path
from typing import Literal, Tuple

import pandas as pd


def fiscal_from_calendar_year(month_num: int, calendar_year: int) -> int:
    """Return the fiscal year for the input calendar year."""

    # Calendar == fiscal year if month is Jan to June (< 7)
    return calendar_year if month_num < 7 else calendar_year + 1


def fiscal_year_quarter_from_path(path: Path) -> Tuple[int, int]:
    """Extract the fiscal year and quarter from the file path."""

    # Match the FYXX_QX pattern
    pattern = "FY(?P<fy>[0-9]{2})[_-]Q(?P<q>[1234])"
    match = re.match(pattern, path.stem)
    if match:
        d = match.groupdict()
    else:
        raise RuntimeError(f"Cannot match FYXX_QX pattern in '{path.stem}'")

    fiscal_year = int(f"20{d['fy']}")
    quarter = int(d["q"])
    return fiscal_year, quarter


def rename_tax_rows(
    df: pd.DataFrame,
    index: int,
    tax_names: list[str],
    suffixes: list[str] = ["current", "prior", "total"],
) -> int:
    """Internal function that loops over consecutive rows and adds the name."""

    for tax_name in tax_names:
        for offset in [0, 1, 2]:
            suffix = suffixes[offset]
            df.loc[index + offset, 0] = f"{tax_name}_{suffix}"
        index = index + 3

    return index


def get_index_label(
    df: pd.DataFrame,
    pattern: str,
    column: str = "0",
    how: Literal["startswith", "contains"] = "startswith",
) -> int | str:
    """Get index label matching a pattern"""

    # Do the selection
    if how == "startswith":
        sel = df[column].str.strip().str.startswith(pattern, na=False)
    else:
        sel = df[column].str.strip().str.contains(pattern, na=False)

    sub = df.loc[sel]
    if len(sub) != 1:
        raise ValueError("Multiple matches for index label")
    return sub.index[0]
