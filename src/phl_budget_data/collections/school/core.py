import calendar
import re
from operator import attrgetter
from typing import List

import numpy as np
import pandas as pd
import pdfplumber

from ... import DATA_DIR
from ...utils.misc import fiscal_from_calendar_year, rename_tax_rows
from ...utils.pdf import extract_words, fuzzy_groupby
from ..base import MonthlyCollectionsReport


def get_collections_report_columns(month: int, calendar_year: int) -> List[str]:
    """Columns for monthly collection reports."""

    # Get the FY from the calendar year
    fiscal_year = fiscal_from_calendar_year(month, calendar_year)

    # Get the month name
    month_name = calendar.month_abbr[month].lower()

    # Fiscal year tsags
    this_year = f"fy{str(fiscal_year)[2:]}"
    last_year = f"fy{str(fiscal_year-1)[2:]}"

    return [
        f"{month_name}_{this_year}",
        f"{month_name}_{last_year}",
        f"{this_year}_ytd",
        f"{last_year}_ytd",
        "net_change",
        "budget_requirement",
        "pct_budgeted",
    ]


class SchoolTaxCollections(MonthlyCollectionsReport):
    """
    Monthly School District Collections Report.

    Parameters
    ----------
    month :
        the calendar month number (starting at 1)
    year :
        the calendar year

    Attributes
    ----------
    path : Path
        the name of the path to the raw PDF file
    num_pages : int
        the number of pages in the PDF
    legacy : bool
        whether the PDF uses a legacy format
    month_name : str
        the 3-letter abbreviation for the month name
    """

    report_type = "school"

    @property
    def legacy(self) -> bool:
        """Whether the format is the legacy or current version."""
        return self.num_pages > 1

    def extract(self) -> pd.DataFrame:
        """Internal function to parse the contents of a legacy PDF page."""

        # Open the PDF document
        with pdfplumber.open(self.path) as pdf:

            out = []
            ncols = None

            # Loop over each page
            for pg in pdf.pages:

                # Determine crop areas
                all_words = extract_words(
                    pg, keep_blank_chars=True, x_tolerance=2, y_tolerance=1
                )

                ## TOP LEFT
                real_estate = [
                    w
                    for w in all_words
                    if w.text.strip().lower().startswith("real estate")
                ]
                top_left = min(real_estate, key=lambda w: w.x0)

                ## BOTTOM LEFT
                total_revenue = [
                    w
                    for w in all_words
                    if w.text.strip().lower().startswith("total revenue")
                ]
                bottom_left = min(total_revenue, key=lambda w: w.x0)

                # Crop the main part of the document and extract the words
                cropped = pg.crop(
                    [top_left.x0, top_left.top, pg.width, bottom_left.bottom]
                )
                words = extract_words(
                    cropped, keep_blank_chars=True, x_tolerance=2, y_tolerance=1
                )

                # Group the words into rows
                rows = fuzzy_groupby(words, lower_tol=5, upper_tol=5, key="bottom")
                rows = {key: sorted(rows[key], key=attrgetter("x0")) for key in rows}

                # Separate out the headers in the header column
                headers = []
                for k in rows:

                    # Headers have alpha letter first
                    first = rows[k][0].text
                    if first[0].strip().isalpha():
                        headers.append(rows[k][0])

                # Set up the grid: nrows by ncols
                nrows = len(headers)
                ncols = None
                rows = []
                for i, h in enumerate(headers):

                    s = pg.crop(
                        [
                            h.x1 + 1,
                            h.top,
                            pg.width,
                            h.bottom + 1,
                        ]
                    ).extract_text()

                    text = re.findall(
                        "(\d+(?:\.\d+)?%|\d{1,3}(?:,\d{3})*|-|N/A)",
                        s.replace(" ", ""),
                    )

                    if ncols is None:
                        ncols = len(text)
                    else:
                        if ncols < len(text):
                            assert False, "Bad"
                        elif ncols > len(text):
                            diff = ncols - len(text)

                            s = s.replace(
                                max(re.findall("\s{2,}", s), key=lambda x: len(x)),
                                " - " * diff,
                            )

                            text = re.findall(
                                "(\d+(?:\.\d+)?%|\d{1,3}(?:,\d{3})*|-|N/A)",
                                s.replace(" ", ""),
                            )

                    assert len(text) == ncols
                    rows.append(text)

                # Initialize the grid
                grid = np.empty((nrows, ncols + 1), dtype=object)
                grid[:, :] = ""  # Default value

                # Add in the headers
                grid[:, 0] = [h.text for h in headers]

                # Loop over each column
                for row_num, this_row in enumerate(rows):
                    grid[row_num, 1:] = this_row

                # Save it
                out.append(pd.DataFrame(grid))

            # Return concatenation
            return pd.concat(out, axis=0, ignore_index=True)

    def transform(self, data):
        """Transform the raw parsing data into a clean data frame."""

        # Call base transform
        data = super().transform(data)

        # Determine columns for the report
        columns = get_collections_report_columns(self.month, self.year)

        ncols = len(data.columns)
        assert ncols in [11, 12, 14]

        if ncols == 14:
            data = data.drop(labels=[7, 8, 9, 10], axis=1)
        else:
            data = data.drop(labels=[data.columns[-6]], axis=1)

        # Set the columns
        columns = ["name"] + columns[-7:]
        data = data[[0] + list(data.columns[-7:])]

        assert len(columns) == len(data.columns)
        assert len(data) in [14, 15]

        # Do current/prior/total
        index = rename_tax_rows(
            data,
            0,
            ["real_estate", "school_income", "use_and_occupancy", "liquor"],
        )

        # PILOTS (optional)
        if len(data) == 15:
            data.loc[index, 0] = "pilots_total"
            index += 1

        # Other non-tax
        data.loc[index, 0] = "other_nontax_total"
        index += 1

        # Total
        data.loc[index, 0] = "total_revenue_total"
        index += 1

        # Set the columns
        data.columns = columns

        # Split out current/prior/total into its own column
        data["kind"] = data["name"].apply(lambda x: x.split("_")[-1])
        data["name"] = data["name"].apply(lambda x: "_".join(x.split("_")[:-1]))

        return data

    def validate(self, data):
        """Validate the input data."""

        # Sum up
        t = data.query("kind == 'total' and name != 'total_revenue'")
        t = t.filter(regex=f"^{self.month_name}", axis=1)

        # Compare to total
        for col in t.columns:
            total_revenue = data.query("name == 'total_revenue'")[col].squeeze()
            diff = t[col].sum() - total_revenue
            assert diff < 5

        return True

    def load(self, data) -> None:
        """Load the data."""

        path = (
            DATA_DIR
            / "processed"
            / "monthly-collections"
            / self.report_type
            / f"{self.year}-{self.month:02d}-tax.csv"
        )
        if not path.parent.exists():
            path.parent.mkdir(parents=True)

        # Save
        data.to_csv(path, index=False)

        return None
