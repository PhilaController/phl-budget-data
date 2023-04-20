import calendar
from dataclasses import dataclass
from operator import attrgetter
from pathlib import Path
from typing import Literal, Optional

import numpy as np
import pandas as pd
import pdfplumber

from ... import ETL_DATA_DIR, ETL_DATA_FOLDERS
from ...core import ETLPipeline
from ...utils import transformations as tr
from ...utils.pdf import extract_words, fuzzy_groupby

CATEGORIES = [
    "General Commercial (88-2)",
    "Office Buildings, Hotels, and Garages (88-3)",
    "Industrial (88-4)",
    "Other Nonresidential (88-5,88-6,77,78)",
    "Nonresidential",
    "Condominiums (88-8)",
    "Apartments (88-1)",
    "Single/Multi-family Homes (01 thur 76)",
    "Residential",
    "Unclassified",
    "Total",
]


@dataclass
class RTTCollectionsBySector(ETLPipeline):  # type: ignore
    """
    Monthly RTT collections by sector.

    Parameters
    ----------
    year :
        the calendar year
    month :
        the calendar month number (starting at 1)
    quarter:
        the calendar quarter (1, 2, 3, 4)
    """

    year: int
    month: Optional[int] = None
    quarter: Optional[Literal[1, 2, 3, 4]] = None

    def __post_init__(self) -> None:
        """Set up necessary variables."""

        if self.month is None and self.quarter is None:
            raise ValueError("Either month or quarter must be specified")

        # The PDF path
        if self.month is not None:
            self.path = (
                self.get_data_directory("raw") / f"{self.year}_{self.month:02d}.pdf"
            )
            if not self.path.exists():
                raise FileNotFoundError(
                    f"No PDF available for month '{self.month}' and year '{self.year}'"
                )

        else:
            self.path = (
                self.get_data_directory("raw") / f"{self.year}_Q{self.quarter}.pdf"
            )
            if not self.path.exists():
                raise FileNotFoundError(
                    f"No PDF available for quarter '{self.quarter}' and year '{self.year}'"
                )

        # Number of pages
        with pdfplumber.open(self.path) as pdf:
            self.num_pages = len(pdf.pages)

        # Rename columns to show quarter
        if self.quarter is not None:
            month_start = (self.quarter - 1) * 3 + 1
            month_name_start = calendar.month_abbr[month_start].lower()

            month_end = month_start + 2
            month_name_end = calendar.month_abbr[month_end].lower()

            self.month_name = f"{month_name_start}_to_{month_name_end}"
        elif self.month is not None:
            # Month name
            self.month_name = calendar.month_abbr[self.month].lower()

    @classmethod
    def get_data_directory(cls, kind: ETL_DATA_FOLDERS) -> Path:
        """Internal function to get the file path.

        Parameters
        ----------
        kind : {'raw', 'processed'}
            type of data to load
        """
        return ETL_DATA_DIR / kind / "collections" / "by-sector" / "rtt"

    def extract(self) -> pd.DataFrame:
        """Extract the data from the first PDF page."""

        # Open the PDF document
        with pdfplumber.open(self.path) as pdf:

            # Only need first page
            pg = pdf.pages[0]

            # Determine crop areas
            all_words = extract_words(
                pg, keep_blank_chars=True, x_tolerance=1, y_tolerance=1
            )

            ## TOP LEFT
            top_left = min(
                [
                    w
                    for w in all_words
                    if w.text.strip().lower().startswith("non-residential")
                ],
                key=lambda w: w.x0,
            )

            # Crop the main part of the document and extract the words
            cropped = pg.crop([top_left.x0, top_left.top, pg.bbox[2], pg.bbox[3]])
            words = extract_words(
                cropped, keep_blank_chars=True, x_tolerance=2, y_tolerance=1
            )

            # Group into rows
            d = []
            for k, v in fuzzy_groupby(words, lower_tol=1, upper_tol=1).items():
                d.append([w.text for w in sorted(v, key=attrgetter("x0"))])

            # Dataframe
            df = pd.DataFrame(data=d)

            # Check if columns got merged because of overlap
            sel = df.index[df[1].str.contains("$", regex=False, na=False)]
            for label in sel:

                row = df.loc[label]
                if row[0].startswith("Other Non-residential"):
                    fields = row[0].split()
                    values = [" ".join(fields[:-1]), fields[-1]]
                    values += row.iloc[1:-1].tolist()
                    df.loc[label, :] = values

            # Return
            return df

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform the raw parsing data into a clean data frame."""

        # Transform
        data = (
            data.pipe(tr.remove_spaces)
            .pipe(tr.fix_percentages)
            .pipe(tr.replace_missing_cells)
            .pipe(tr.convert_to_floats, usecols=data.columns[1:])
            .pipe(tr.remove_missing_rows, usecols=data.columns[1:])
        ).reset_index(drop=True)

        # Get first 11
        data = data.iloc[:11]

        # Only need first three columns if >2019
        if self.year > 2019:
            data = data[[0, 1, 2]]
        else:
            data = data[[0, 7, 8]]

        # Rename columns
        data.columns = ["category", "num_records", "total"]

        # Fill zeroes
        for col in data.columns[1:]:
            data[col] = data[col].fillna(0)

        # Assign uniform industries
        data["category"] = CATEGORIES
        data["parent_category"] = np.select(
            [
                data["category"].isin(
                    [
                        "General Commercial (88-2)",
                        "Office Buildings, Hotels, and Garages (88-3)",
                        "Industrial (88-4)",
                        "Other Nonresidential (88-5,88-6,77,78)",
                    ]
                ),
                data["category"].isin(
                    [
                        "Condominiums (88-8)",
                        "Apartments (88-1)",
                        "Single/Multi-family Homes (01 thur 76)",
                    ]
                ),
            ],
            [
                "Nonresidential",
                "Residential",
            ],
            default="",
        )
        data["parent_category"] = data["parent_category"].replace("", np.nan)

        # Return
        return data

    def validate(self, data: pd.DataFrame) -> bool:
        """Validate the input data."""

        cols = ["num_records", "total"]
        subsectors = data.query("parent_category.notnull()")
        totals = subsectors.groupby("parent_category")[cols].sum()

        # Check subcategories
        for col in cols:
            for category in totals.index:
                total1 = totals.loc[category, col]
                total2 = data.loc[data["category"] == category][col].squeeze()
                diff = total1 - total2
                assert diff < 5

        # Check main categories
        categories = ["Residential", "Nonresidential", "Unclassified"]
        A = data.query("category in @categories")[cols].sum()
        B = data.query("category =='Total'").squeeze()[cols]
        assert ((A - B) < 5).all()

        return True

    def load(self, data: pd.DataFrame) -> None:
        """Load the data."""

        # Path to save data to
        dirname = self.get_data_directory("processed")
        if self.month is not None:
            path = dirname / f"{self.year}-{self.month:02d}.csv"
        else:
            path = dirname / f"{self.year}-Q{self.quarter}.csv"

        # Load
        super()._load_csv_data(data, path)