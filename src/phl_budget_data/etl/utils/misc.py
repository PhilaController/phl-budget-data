import re


def fiscal_from_calendar_year(month_num, calendar_year):
    """Return the fiscal year for the input calendar year."""

    return calendar_year if month_num < 7 else calendar_year + 1


def rename_tax_rows(df, index, tax_names, suffixes=["current", "prior", "total"]):
    """Internal function that loops over consecutive rows and adds the name."""

    for tax_name in tax_names:
        for offset in [0, 1, 2]:
            suffix = suffixes[offset]
            df.loc[index + offset, 0] = f"{tax_name}_{suffix}"
        index = index + 3

    return index


def get_index_label(df, pattern, column="0", how="startswith"):
    """Get index label matching a pattern"""

    assert how in ["startswith", "contains"]
    if how == "startswith":
        sel = df[column].str.strip().str.startswith(pattern, na=False)
    else:
        sel = df[column].str.strip().str.contains(pattern, na=False)

    sub = df.loc[sel]
    if len(sub) != 1:
        raise ValueError("Multiple matches for index label")
    return sub.index[0]


def fiscal_year_quarter_from_path(path):
    """Extract the fiscal year and quarter from the file path."""

    pattern = "FY(?P<fy>[0-9]{2})[_-]Q(?P<q>[1234])"
    matches = re.match(pattern, path.stem).groupdict()

    fiscal_year = int(f"20{matches['fy']}")
    quarter = int(matches["q"])
    return fiscal_year, quarter
