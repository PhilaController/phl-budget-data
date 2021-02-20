import calendar
from operator import attrgetter
from typing import List

import numpy as np
import pandas as pd
import pdfplumber

from ...utils.misc import fiscal_from_calendar_year
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
        f"{last_year}_actual",
        f"{this_year}_budgeted",
        f"{month_name}_{this_year}",
        f"{month_name}_{last_year}",
        f"{this_year}_ytd",
        f"{last_year}_ytd",
        "net_change",
        "budget_requirement",
        "pct_budgeted",
    ]


def _find_footer_cutoff(pg):
    """Search for a horizontal line separating footer from data."""

    rects = [r for r in pg.rects if r["width"] / pg.width > 0.5]
    max_rect = max(rects, key=lambda r: r["bottom"])
    if max_rect["bottom"] > 0.9 * float(pg.height):
        return max_rect
    else:
        return None


def _find_header_column_cutoff(pdf):
    """Search for a horizontal line separating first column of headers from
    the data."""

    x0 = min(pdf.pages[0].rects, key=lambda r: r["x0"])["x0"]
    rects = []
    for pg in pdf.pages:
        rects += [
            r
            for r in pg.rects
            if r["width"] / pg.width < 0.5 and abs(r["x0"] - x0) < 30
        ]

    max_rect = max(rects, key=lambda r: r["width"])
    return max_rect["x1"]


class CityCollectionsReport(MonthlyCollectionsReport):
    """
    Monthly City Collections Report.

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

    report_type = "city"

    @property
    def legacy(self):
        """Whether the format is the legacy or current version."""
        return self.num_pages > 4

    def extract(self) -> pd.DataFrame:
        """Parse and extract the contents of a legacy PDF page."""

        # Open the PDF document
        with pdfplumber.open(self.path) as pdf:

            # Determine the verical line separating header column from rest
            # only valid in modern layout
            if not self.legacy:
                header_column_cutoff = _find_header_column_cutoff(pdf)
            else:
                header_column_cutoff = 0

            out = []
            ncols = None

            # Loop over each page
            for pg in pdf.pages:

                # Extract text to look for a header
                pg_text = pg.extract_text()

                # This shows up in the header columns
                has_header = "FISCAL YEAR TO DATE" in pg_text
                if has_header:

                    # Determine if header has 1 or 2 horizontal lines
                    # We want to find the bottom horizontal line to trim
                    rects = sorted(pg.rects, key=lambda x: x["y0"], reverse=True)
                    if "Comparative Statement" in pg_text:
                        top = rects[1]["bottom"]
                    else:
                        top = rects[0]["bottom"]
                else:
                    top = 0  # Start at top of page if no header

                # Is there a width-spanning line at the bottom?
                footer_cutoff = _find_footer_cutoff(pg)
                if footer_cutoff is not None:
                    bottom = footer_cutoff["bottom"]
                else:
                    bottom = pg.height

                # Use first column text in the header to determine width of header column
                if not self.legacy and has_header:
                    header_column_cutoff = (
                        min(
                            pg.crop([0, 0, pg.width, top]).extract_words(),
                            key=lambda w: w["x0"],
                        )["x0"]
                        - 1  # Add a small pad
                    )

                # Crop the main part of the document and extract the words
                cropped = pg.crop([header_column_cutoff, top, pg.width, bottom])
                words = extract_words(
                    cropped, keep_blank_chars=True, x_tolerance=1, y_tolerance=1
                )

                # Check for footnotes
                footnote_cutoff = None
                for word in words:
                    if word.text.startswith("*"):
                        footnote_cutoff = word.top

                if footnote_cutoff is not None:
                    words = [w for w in words if w.bottom < footnote_cutoff]

                # Get the header column (if not legacy)
                if not self.legacy:
                    header_part = pg.crop([0, top, header_column_cutoff, bottom])
                    headers = extract_words(header_part, keep_blank_chars=True)
                else:  # In the legacy format, we need to manually identify headers

                    # Group the words into rows
                    rows = fuzzy_groupby(words, lower_tol=5, upper_tol=5, key="bottom")
                    rows = {
                        key: sorted(rows[key], key=attrgetter("x0")) for key in rows
                    }

                    # Separate out the headers in the header column
                    words = []
                    headers = []
                    for k in rows:

                        # Headers have alpha letter first
                        first = rows[k][0].text
                        if first[0].strip().isalpha():
                            headers.append(rows[k][0])
                        else:
                            words += [w for w in rows[k]]

                # Now, group into columns by right most point in bbox
                columns = fuzzy_groupby(words, lower_tol=10, upper_tol=10, key="x1")

                # Remove any columns that are full subsets of another column
                # This can happen when fuzzy matching to do the columns
                remove = []
                for k in columns:
                    values = columns[k]
                    if any(
                        [
                            all(val in columns[j] for val in values)
                            for j in columns
                            if j != k
                        ]
                    ):
                        remove.append(k)
                columns = {k: columns[k] for k in columns if k not in remove}

                # Remove any orphan columns that are too close together
                # Sometimes columns will end up split when they should be merged
                MIN_COL_SEP = 24
                locations = sorted(columns)
                for i in range(len(locations) - 1):
                    this_loc = locations[i]
                    next_loc = locations[i + 1]

                    if this_loc not in columns:
                        continue

                    if next_loc - this_loc < MIN_COL_SEP:
                        if len(columns[next_loc]) > len(columns[this_loc]):
                            columns[next_loc] += columns[this_loc]
                            del columns[this_loc]
                        else:
                            columns[this_loc] += columns[next_loc]
                            del columns[next_loc]

                # Sort columns vertically from top to bottom
                columns = {
                    key: sorted(columns[key], key=attrgetter("top")) for key in columns
                }

                # Text in header column are sometimes split, leaving parts of words
                # in an orphan column — this checks if columns are all alpha
                # and removes them
                for key in sorted(columns):
                    value = columns[key]
                    if all([w.text.replace(" ", "").isalpha() for w in value]):
                        del columns[key]

                # Store the bottom y values of all of the row headers
                header_tops = np.array([h.top for h in headers])

                # Set up the grid: nrows by ncols
                nrows = len(headers)
                if ncols is None:
                    ncols = len(columns) + 1
                else:
                    assert ncols == len(columns) + 1, columns

                # Initialize the grid
                grid = np.empty((nrows, ncols), dtype=object)
                grid[:, :] = ""  # Default value

                # Add in the headers
                grid[:, 0] = [h.text for h in headers]

                # Loop over each column
                for col_num, xval in enumerate(columns):

                    col = columns[xval]
                    word_tops = np.array([w.top for w in col])

                    # Find closest row header
                    match_tol = 20
                    for row_num, h in enumerate(headers):

                        # Find closest word ot this row heasder
                        word_diff = np.abs(word_tops - h.top)
                        word_diff[word_diff > match_tol] = np.nan

                        # Make sure the row header is vertically close enough
                        if np.isnan(word_diff).sum() < len(word_diff):

                            # Get the matching word for this row header
                            notnull = ~np.isnan(word_diff)
                            order = np.argsort(word_diff[notnull])
                            for word_index in np.where(notnull)[0][order]:
                                word = col[word_index]

                                # IMPORTANT: make sure this is the closest row header
                                # Sometimes words will match to more than one header
                                header_diff = np.abs(header_tops - word.top)
                                header_index = np.argmin(header_diff)
                                closest_header = headers[header_index]
                                if closest_header == h:
                                    grid[row_num, col_num + 1] = col[word_index].text
                                    break

                # Save it
                out.append(pd.DataFrame(grid))

            # Return concatenation
            return pd.concat(out, axis=0, ignore_index=True)
