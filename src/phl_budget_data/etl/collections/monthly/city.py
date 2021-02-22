import numpy as np
import pandas as pd
import pdfplumber

from ...utils.pdf import extract_words, words_to_table
from ...utils.transformations import remove_empty_columns
from .core import MonthlyCollectionsReport


def find_top_cutoff(pg):
    """Search for the top cutoff of header on the page."""

    # Extract text to look for a header
    pg_text = pg.extract_text()

    # This shows up in the header columns
    top = 0
    has_header = "FISCAL YEAR TO DATE" in pg_text
    if has_header:

        # Determine if header has 1 or 2 horizontal lines
        # We want to find the bottom horizontal line to trim
        rects = sorted(pg.rects, key=lambda x: x["y0"], reverse=True)
        if "Comparative Statement" in pg_text:
            top = rects[1]["bottom"]
        else:
            top = rects[0]["bottom"]

    return top


def find_footer_cutoff(pg):
    """Search for a horizontal line separating footer from data."""

    rects = [r for r in pg.rects if r["width"] / pg.width > 0.5]
    max_rect = max(rects, key=lambda r: r["bottom"])
    if max_rect["bottom"] > 0.9 * float(pg.height):
        return max_rect
    else:
        return None


class CityCollectionsReport(MonthlyCollectionsReport):
    """
    Monthly City Collections Report.

    Parameters
    ----------
    month :
        the calendar month number (starting at 1)
    year :
        the calendar year
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

            # Loop over each page
            out = []
            for pg in pdf.pages:

                # Is there a width-spanning line at the top?
                top = find_top_cutoff(pg)

                # Is there a width-spanning line at the bottom?
                footer_cutoff = find_footer_cutoff(pg)
                if footer_cutoff is not None:
                    bottom = footer_cutoff["bottom"]
                else:
                    bottom = pg.height

                # Crop the main part of the document and extract the words
                cropped = pg.crop([0, top, pg.width, bottom])
                words = extract_words(
                    cropped, keep_blank_chars=False, x_tolerance=1, y_tolerance=1
                )

                # Group the words into a table
                data = words_to_table(
                    words,
                    text_tolerance_y=5,
                    text_tolerance_x=5,
                    column_tolerance=20,
                    min_col_sep=24,
                    header_column_overlap=20,
                )

                # Remove first row of header if we need to
                for phrase in ["prelim", "final", "budget"]:
                    sel = data[0].str.lower().str.startswith(phrase)
                    data = data.loc[~sel]

                # Remove empty columns
                data = remove_empty_columns(data, use_nan=False)

                # Check number of columns
                if len(out):
                    if len(data.columns) != len(out[-1].columns):
                        raise ValueError("Column mismatch when parsing multiple pages")

                # Save it
                out.append(data)

            # Return concatenation
            return pd.concat(out, axis=0, ignore_index=True)
