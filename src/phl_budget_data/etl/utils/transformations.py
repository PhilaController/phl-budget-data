"""Transformation utility functions."""
import re
from typing import List, Literal, Optional

import numpy as np
import pandas as pd


def remove_unwanted_chars(x: str, *chars: str, to_replace: str = "") -> str:
    """Remove unwanted characters from a string."""
    return re.sub(f"[{''.join(chars)}]", to_replace, x)


def remove_parentheses(x: str, to_replace: str = "") -> str:
    """Remove parentheses from a string."""
    return re.sub(r"\([^)]*\)", to_replace, x)


def decimal_to_comma(
    df: pd.DataFrame, usecols: Optional[list[str]] = None
) -> pd.DataFrame:
    """Convert decimal values to commas."""

    if usecols is None:
        usecols = df.columns

    for col in usecols:
        df[col] = df[col].astype(str).str.replace(".", ",", regex=False)

    return df


def fix_zeros(df: pd.DataFrame, usecols: Optional[list[str]] = None) -> pd.DataFrame:
    """Convert O and o to 0."""

    if usecols is None:
        usecols = df.columns

    for col in usecols:
        df[col] = (
            df[col].astype(str).str.replace("o", "0", regex=True, flags=re.IGNORECASE)
        )

    return df


def replace_commas(
    df: pd.DataFrame, usecols: Optional[List[str]] = None
) -> pd.DataFrame:
    """Replace commas with a period."""

    if usecols is None:
        usecols = df.columns

    def rreplace(s: str) -> str:
        if re.match(".*,[0-9]$", s):
            s_reversed = s[::-1].replace(",", ".", 1)
            return s_reversed[::-1]
        else:
            return s

    for col in usecols:
        df[col] = df[col].astype(str).apply(rreplace)

    return df


def fix_decimals(
    df: pd.DataFrame,
    usecols: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Fix parsing errors related to floating point decimals."""

    if usecols is None:
        usecols = df.columns

    # Non-decimal
    non_decimal = re.compile(r"[^\d()-]+")

    # Add decimal
    numbers = re.compile("(\d+)?")

    def add_decimal(s: str) -> str:
        matches = list(filter(lambda s: len(s.strip()), numbers.findall(s)))
        if not len(matches) == 1:
            return s
        match = matches[0]
        s = s.replace(match, f"{match[:-1]}.{match[-1]}")
        return s

    for col in usecols:
        df[col] = df[col].fillna("").apply(lambda s: non_decimal.sub("", s))
        df[col] = df[col].apply(add_decimal)

    return df


def convert_to_floats(
    df: pd.DataFrame,
    usecols: Optional[List[str]] = None,
    errors: Literal["coerce", "raise", "ignore"] = "coerce",
) -> pd.DataFrame:
    """
    Convert string values in currency format to floats.

    Parameters
    ----------
    df :
        the data to format
    usecols :
        an optional list of columns to convert
    errors : {‘ignore’, ‘raise’, ‘coerce’}, default ‘raise’
        - If ‘raise’, then invalid parsing will raise an exception.
        - If ‘coerce’, then invalid parsing will be set as NaN.
        - If ‘ignore’, then invalid parsing will return the input.
    """
    if usecols is None:
        usecols = df.columns

    for col in usecols:
        df[col] = pd.to_numeric(
            df[col].replace("[\$,)]", "", regex=True).replace("[(]", "-", regex=True),
            errors=errors,
        )
    return df


def remove_footnotes(df: pd.DataFrame) -> pd.DataFrame:
    """Remove any lines starting with an asterisk."""
    is_footnote = df[0].str.strip().str.startswith("*")
    return df.loc[~is_footnote].copy()


def replace_missing_cells(df: pd.DataFrame) -> pd.DataFrame:
    """Fill empty or N/A values with NaN values."""

    df = df.replace("", np.nan)
    for col in df.columns[1:]:
        df[col] = df[col].replace("N/A", np.nan)
        df[col] = df[col].replace("-", np.nan)

    return df


def fix_percentages(df: pd.DataFrame) -> pd.DataFrame:
    """Strip percent signs from the end of cell values."""
    for col in df.columns[1:]:
        df[col] = df[col].str.strip().str.rstrip("%")
    return df


def strip_dollar_signs(df: pd.DataFrame) -> pd.DataFrame:
    """Strip dollars signs from the start of cell values."""
    for col in df.columns[1:]:
        df[col] = df[col].str.strip().str.lstrip("$")
    return df


def remove_spaces(df: pd.DataFrame) -> pd.DataFrame:
    """Strip out spaces from cells."""
    for col in df.columns[1:]:
        df[col] = df[col].str.replace("\s+", "", regex=True)
    return df


def fix_duplicate_parens(df: pd.DataFrame) -> pd.DataFrame:
    """Change any duplicate parentheses to single only."""
    for col in df.columns[1:]:
        df[col] = df[col].str.replace("((", "(", regex=False)
        df[col] = df[col].str.replace("))", ")", regex=False)
    return df


def remove_missing_rows(
    df: pd.DataFrame, usecols: Optional[list[str]] = None
) -> pd.DataFrame:
    """Remove rows that are empty."""
    if usecols is None:
        usecols = df.columns
    missing = df[usecols].isnull().all(axis=1)
    return df.loc[~missing]


def remove_extra_letters(df: pd.DataFrame, col_num: int) -> pd.DataFrame:
    """Remove any letters mixed into the first column."""
    col = df.columns[col_num]
    df[col] = df[col].str.replace("[A-Z]", "", regex=True)
    return df


def fix_duplicated_chars(data: pd.DataFrame) -> pd.DataFrame:
    """Check for duplicated characters."""

    # Check for rows where characters are duplicated
    first = data[1][data[1].str.len() > 0]
    first = first.str.replace(r"([a-zA-Z0-9%,.])\1+", r"", regex=True).str.replace(
        ",", ""
    )

    # Use regex to remove any cells that have characters duplicated
    duplicated = first.loc[first.str.len() == 0].index
    for index in duplicated:
        data.loc[index] = data.loc[index].str.replace(
            r"([a-zA-Z0-9%,.])\1{1,2}", r"\1", regex=True
        )

    return data


def remove_empty_columns(data: pd.DataFrame, use_nan: bool = False) -> pd.DataFrame:
    """Remove empty columns."""

    # Remove null columns
    if use_nan:
        bad_columns = data.isnull().all()
    else:
        bad_columns = (data == "").all()

    if bad_columns.sum():
        data = data[data.columns[~bad_columns]]
        data.columns = list(range(0, len(data.columns)))

    return data
