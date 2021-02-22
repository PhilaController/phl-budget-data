import itertools
import re
from dataclasses import dataclass
from operator import attrgetter
from typing import Dict, Iterator, List, Optional, Type, TypeVar

import desert
import marshmallow
import numpy as np
import pandas as pd
import pdfplumber
from intervaltree import IntervalTree

# Create a generic variable that can be 'Parent', or any subclass.
Word_T = TypeVar("Word_T", bound="Word")


def extract_words(
    pg: pdfplumber.page.Page,
    keep_blank_chars: bool = False,
    x_tolerance: int = 2,
    y_tolerance: int = 2,
):
    words = []

    for word_dict in pg.extract_words(
        keep_blank_chars=keep_blank_chars,
        x_tolerance=x_tolerance,
        y_tolerance=y_tolerance,
    ):

        # Convert to a Word
        word = Word.from_dict(word_dict)

        # Clean up text
        word.text = word.text.replace("\xa0", " ").strip()

        # Save it
        if word.text:
            words.append(word)

    # Sort the words top to bottom and left to right
    return sorted(words, key=attrgetter("top", "x0"), reverse=False)


@dataclass
class Word:
    """
    A word in the PDF with associated text and bounding box.

    Parameters
    ----------
    x0 :
        the starting horizontal coordinate
    x1 :
        the ending horizontal coordinate
    bottom :
        the bottom vertical coordinate
    top :
        the top vertical coordinate
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
        """Alias for `tops`."""
        return self.top

    @classmethod
    def from_dict(cls: Type[Word_T], data: dict) -> Word_T:
        """
        Return a new class instance from a dictionary
        representation.

        Parameters
        ----------
        data :
            The dictionary representation of the class.
        """
        schema = desert.schema(cls, meta={"unknown": marshmallow.EXCLUDE})
        return schema.load(data)


def groupby(words: List[Word], key: str, sort: bool = False) -> Iterator:
    """Group words by the specified attribute, optionally sorting."""
    if sort:
        words = sorted(words, key=attrgetter(key))
    return itertools.groupby(words, attrgetter(key))


def fuzzy_groupby(
    words: List[Word], lower_tol: int = 10, upper_tol: int = 10, key="y"
) -> Dict[float, List[Word]]:
    """Group words into lines, with a specified tolerance."""

    tree = IntervalTree()
    for i in range(len(words)):
        y = getattr(words[i], key)
        tree[y - lower_tol : y + upper_tol] = words[i]  # type: ignore

    result: Dict[float, List[Word]] = {}
    for y in sorted(np.unique([getattr(w, key) for w in words])):
        objs = [iv.data for iv in tree[y]]
        values = sorted(objs, key=attrgetter("x"))

        if values not in result.values():
            result[y] = values

    return result


def create_data_table(headers, columns, match_tol=20) -> pd.DataFrame:
    """Based on headers and column data, create the data table."""

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


def remove_all_alpha_columns(columns):
    """Remove orphan columns that are all words and no numbers."""
    for key in sorted(columns):
        value = columns[key]
        if all([w.text.replace(" ", "").isalpha() for w in value]):
            del columns[key]
    return columns


def remove_close_columns(columns, min_col_sep=24):
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


def remove_orphan_columns(columns):
    """Remove any columns that are full subsets of another column."""
    remove = []
    for k in columns:
        values = columns[k]
        if any([all(val in columns[j] for val in values) for j in columns if j != k]):
            remove.append(k)

    return {k: columns[k] for k in columns if k not in remove}


def remove_hyphens(df):

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
    header_column_overlap: int = 10,
) -> pd.DataFrame:
    """Combine words into a table, returning a pandas DataFrame"""

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
    table = create_data_table(headers, columns, match_tol=header_column_overlap)

    return remove_hyphens(table)
