import itertools
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
