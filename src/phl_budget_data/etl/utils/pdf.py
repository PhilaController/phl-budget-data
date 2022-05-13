"""Module for extracting data from PDFs."""
import itertools
import re
from operator import attrgetter
from typing import Dict, Iterator, List, Optional

import numpy as np
import pandas as pd
import pdfplumber
from intervaltree import IntervalTree
from pydantic import BaseModel


class Word(BaseModel):
    """
    A word in the PDF with associated text and bounding box.

    Parameters
    ----------
    x0 :
        the starting horizontal coordinate
    x1 :
        the ending horizontal coordinate
    top :
        the top vertical coordinate
    bottom :
        the bottom vertical coordinate
    text :
        the associated text
    """

    x0: float
    x1: float
    top: float
    bottom: float
    text: str

    @property
    def x(self) -> float:
        """Alias for `x0`."""
        return self.x0

    @property
    def y(self) -> float:
        """Alias for `top`."""
        return self.top


def extract_words(
    pg: pdfplumber.page.Page,
    keep_blank_chars: bool = False,
    x_tolerance: int = 2,
    y_tolerance: int = 2,
) -> List[Word]:
    """
    Extract words from the input page.

    See Also
    --------
    https://github.com/jsvine/pdfplumber#the-pdfplumberpage-class

    Parameters
    ----------
    pg :
        The PDF page to parse
    keep_blank_chars :
        Blank characters are treated as part of a word, not as a space between words.
    x_tolerance :
        The spacing tolerance in the x direction.
    y_tolerance :
        The spacing tolerance in the y direction.
    """
    words = []

    for word_dict in pg.extract_words(
        keep_blank_chars=keep_blank_chars,
        x_tolerance=x_tolerance,
        y_tolerance=y_tolerance,
    ):

        # Convert to a Word
        word = Word(**word_dict)

        # Clean up text
        word.text = word.text.replace("\xa0", " ").strip()

        # Save it
        if word.text:
            words.append(word)

    # Sort the words top to bottom and left to right
    return sorted(words, key=attrgetter("top", "x0"), reverse=False)


def groupby(words: List[Word], key: str, sort: bool = False) -> Iterator:
    """Group words by the specified attribute, optionally sorting."""
    if sort:
        words = sorted(words, key=attrgetter(key))
    return itertools.groupby(words, attrgetter(key))


def fuzzy_groupby(
    words: List[Word], lower_tol: int = 10, upper_tol: int = 10, key: str = "y"
) -> Dict[float, List[Word]]:
    """Group words into lines, with a specified tolerance."""

    tree = IntervalTree()
    for i in range(len(words)):
        y = getattr(words[i], key)
        tree[y - lower_tol : y + upper_tol] = words[i]

    result: Dict[float, List[Word]] = {}
    for y in sorted(np.unique([getattr(w, key) for w in words])):
        objs = [iv.data for iv in tree[y]]
        values = sorted(objs, key=attrgetter("x"))

        if values not in result.values():
            result[y] = values

    return result


def create_data_table(
    headers: List[Word], columns: Dict[float, List[Word]], match_tol: int = 20
) -> pd.DataFrame:
    """Based on row headers and column data, create the data table."""

    # Store the bottom y values of all of the row headers
    header_tops = np.array([h.top for h in headers])

    # Set up the grid: nrows by ncols
    nrows = len(headers)
    ncols = len(columns) + 1

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

    return pd.DataFrame(grid)


def remove_all_alpha_columns(
    columns: Dict[float, List[Word]]
) -> Dict[float, List[Word]]:
    """Remove orphan columns that are all words and no numbers."""
    for key in sorted(columns):
        value = columns[key]
        if all([w.text.replace(" ", "").isalpha() for w in value]):
            del columns[key]
    return columns


def remove_close_columns(
    columns: Dict[float, List[Word]], min_col_sep: int = 24
) -> Dict[float, List[Word]]:
    """Remove columns that are closer than a specified distance."""

    locations = sorted(columns)
    for i in range(len(locations) - 1):
        this_loc = locations[i]
        next_loc = locations[i + 1]

        if this_loc not in columns:
            continue

        if next_loc - this_loc < min_col_sep:
            if len(columns[next_loc]) > len(columns[this_loc]):
                columns[next_loc] += columns[this_loc]
                del columns[this_loc]
            else:
                columns[this_loc] += columns[next_loc]
                del columns[next_loc]

    return columns


def remove_orphan_columns(columns: Dict[float, List[Word]]) -> Dict[float, List[Word]]:
    """Remove any columns that are full subsets of another column."""
    remove = []
    for k in columns:
        values = columns[k]
        if any([all(val in columns[j] for val in values) for j in columns if j != k]):
            remove.append(k)

    return {k: columns[k] for k in columns if k not in remove}


def remove_hyphens(df: pd.DataFrame) -> pd.DataFrame:
    """Remove any hyphens from the input data."""

    df = df.replace("-", "")
    bad_columns = (df == "").all(axis=0)
    if bad_columns.sum():
        df = df[df.columns[~bad_columns]]
        df.columns = list(range(0, len(df.columns)))

    return df


def words_to_table(
    words: List[Word],
    text_tolerance_y: int = 5,
    text_tolerance_x: int = 3,
    column_tolerance: int = 20,
    min_col_sep: int = 24,
    row_header_tolerance: int = 10,
) -> pd.DataFrame:
    """
    Combine words into a table, returning a pandas DataFrame.

    Parameters
    ----------
    words :
        The list of words returned by extract_words()
    text_tolerance_y :
        The tolerance for matching text in the y direction
    text_tolerance_x :
        The tolerance for matching text in the x direction
    column_tolerance :
        The x tolerance when grouping words into columns
    min_col_sep :
        Merge any adjacent columns that are closer than this value
    row_header_tolerance :
        The tolerance when matching row headers to words
    """

    # Make a copy
    words = [word.copy(deep=True) for word in words]

    # Group into words
    rows = fuzzy_groupby(
        words, key="bottom", lower_tol=text_tolerance_y, upper_tol=text_tolerance_y
    )

    # Split headers (first column) from rest
    words = []
    headers = []

    # Loop over phrases
    footnote_cutoff = None
    for k in rows:
        row = rows[k]

        # Group words into phrases
        for i in reversed(range(1, len(row))):
            this_word = row[i]
            prev_word = row[i - 1]

            if this_word.x0 - prev_word.x1 < text_tolerance_x:
                row[i - 1].text += " " + row[i].text
                row[i - 1].x1 = row[i].x1
                del row[i]

        # Skip footnote rows
        first = row[0].text
        if first.startswith("*"):
            continue

        # Split headers from numbers
        if first[0].strip().isalpha():

            # Keep the header
            headers.append(row[0])

            # Keep any non-header numbers
            words += [
                w
                for w in row
                if re.match(
                    "\(?(\d+(?:\.\d+)?%|\d{1,3}(?:,\d{3})*|-|N/A)\)?",
                    w.text.replace(" ", ""),
                )
            ]
        else:
            words += [w for w in row]

    # Group into columns and sort it
    columns = fuzzy_groupby(
        words, lower_tol=column_tolerance, upper_tol=column_tolerance, key="x1"
    )
    columns = {key: sorted(columns[key], key=attrgetter("top")) for key in columns}

    # Remove any columns that are full subsets of another column
    # This can happen when fuzzy matching to do the columns
    columns = remove_orphan_columns(columns)

    # Remove any orphan columns that are too close together
    # Sometimes columns will end up split when they should be merged
    columns = remove_close_columns(columns, min_col_sep=min_col_sep)

    # Text in header column are sometimes split, leaving parts of words
    # in an orphan column — this checks if columns are all alpha
    # and removes them
    columns = remove_all_alpha_columns(columns)

    # Create the table
    table = create_data_table(headers, columns, match_tol=row_header_tolerance)

    # Return with hyphens removed
    return remove_hyphens(table)


def find_phrases(words: List[Word], *keywords: str) -> Optional[List[Word]]:
    """
    Find a list of consecutive words that match the input keywords.

    Parameters
    ----------
    words :
        the list of words to check
    *keywords
        one or more keywords representing the phrase to search for
    """

    # Make sure we have keywords
    assert len(keywords) > 0

    # Iterate through words and check
    for i, w in enumerate(words):

        # Matched the first word!
        if w.text == keywords[0]:

            # Did we match the rest
            match = True
            for j, keyword in enumerate(keywords[1:]):
                if keyword != words[i + 1 + j].text:
                    match = False

            # Match!
            if match:
                return words[i : i + len(keywords)]

    return None


def get_pdf_words(
    pdf_path: str,
    keep_blank_chars: bool = False,
    x_tolerance: int = 3,
    y_tolerance: int = 3,
    footer_cutoff: Optional[int] = None,
    header_cutoff: Optional[int] = None,
) -> List[Word]:
    """
    Parse a PDF and return the parsed words as well as x/y
    locations.

    Parameters
    ----------
    pdf_path :
        the path to the PDF to parse
    keep_blank_chars :
        Blank characters are treated as part of a word, not as a space between words.
    x_tolerance :
        The spacing tolerance in the x direction.
    y_tolerance :
        The spacing tolerance in the y direction.
    footer_cutoff :
        Amount to trim from the bottom
    header_cutoff :
        Amount to trim from the top
    """
    # Get header cutoff
    footer_cutoff_ = footer_cutoff
    if header_cutoff is None:
        header_cutoff = 0

    with pdfplumber.open(pdf_path) as pdf:

        # Loop over pages
        offset = 0
        words = []
        for i, pg in enumerate(pdf.pages):

            if footer_cutoff is None:
                footer_cutoff_ = float(pg.height)

            # Extract out words
            for word_dict in pg.extract_words(
                keep_blank_chars=keep_blank_chars,
                x_tolerance=x_tolerance,
                y_tolerance=y_tolerance,
            ):

                # Convert to a Word
                word = Word(**word_dict)

                # Check header and footer cutoffs
                if word.bottom < footer_cutoff_ and word.top > header_cutoff:

                    # Clean up text
                    word.text = word.text.replace("\xa0", " ").strip()

                    # Add the offset
                    word.top += offset
                    word.bottom += offset

                    # Save it
                    words.append(word)

        # Sort the words top to bottom and left to right
        words = sorted(words, key=attrgetter("top", "x0"), reverse=False)

        return words
