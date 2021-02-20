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
