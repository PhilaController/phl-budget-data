import calendar
from dataclasses import dataclass
from operator import attrgetter

import pdfplumber

from ... import DATA_DIR
from ...etl import ETLPipeline
from ...utils.pdf import extract_words, fuzzy_groupby
from ...utils.transformations import *

SECTORS = [
    "General Commercial",
    "Office Buildings, Hotels and Garages",
    "Industrial",
    "Other Non-Residential",
    "Non-Residential",
    "Condos",
    "Apartments",
    "Single/Multi-family Homes",
    "Residential",
    "Unclassified",
    "Total",
]

QUARTERS = {
    1: [s.lower() for s in calendar.month_abbr[1:4]],
    2: [s.lower() for s in calendar.month_abbr[4:7]],
    3: [s.lower() for s in calendar.month_abbr[7:10]],
    4: [s.lower() for s in calendar.month_abbr[10:]],
}


def month_to_quarter(month):
    """Convert month number to calendar year quarter."""

    if month in [1, 2, 3]:
        return 1
    elif month in [4, 5, 6]:
        return 2
    elif month in [7, 8, 9]:
        return 3
    else:
        return 4


@dataclass
class RTTCollectionsBySector(ETLPipeline):
    """
    Fiscal year real estate transfer (RTT) collections by sector.

    Parameters
    ----------
    month :
        the calendar month number (starting at 1)
    year :
        the calendar year
    """

    month: int
    year: int

    def __post_init__(self):
        """Set up necessary variables."""

        # Which file format?
        self.legacy = self.year < 2020

        # Month name
        self.month_name = calendar.month_abbr[self.month].lower()

        # The PDF path
        if self.legacy:
            quarter = month_to_quarter(self.month)
            self.path = self.get_data_directory("raw") / f"{self.year}_Q{quarter}.pdf"
        else:
            self.path = (
                self.get_data_directory("raw") / f"{self.year}_{self.month:02d}.pdf"
            )

        # Make sure this path exists
        if not self.path.exists():
            raise FileNotFoundError(
                f"No PDF available for month '{self.month}' and year '{self.year}' at '{self.path}'"
            )

    @classmethod
    def get_data_directory(cls, kind: str) -> str:
        """Internal function to get the file path.

        Parameters
        ----------
        kind : {'raw', 'processed'}
            type of data to load
        """
        assert kind in ["raw", "processed"]

        return DATA_DIR / kind / "collections" / "by-sector" / "rtt"

    def extract(self) -> pd.DataFrame:
        """Extract the data from the first PDF page."""

        # Open the PDF document
        with pdfplumber.open(self.path) as pdf:

            # Only need first page
            pg = pdf.pages[0]

            # Extract
            df = pd.DataFrame(
                pg.extract_tables(
                    {
                        "vertical_strategy": "lines",
                        "horizontal_strategy": "lines",
                    }
                )[0],
            )

            if self.legacy:

                quarter = month_to_quarter(self.month)
                index = QUARTERS[quarter].index(self.month_name)

                df = df[[0, 1 + 2 * index, 2 + 2 * index]]

            else:

                df = df[[0, 1, 2]]

            df = df.iloc[2:-1]
            df.columns = ["sector", "num_records", "total"]
            return df

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform the raw parsing data into a clean data frame."""

        # Apply set of base transformations first
        data = (
            data.pipe(remove_spaces)
            .pipe(fix_percentages)
            .pipe(strip_dollar_signs)
            .pipe(replace_missing_cells)
            .pipe(convert_to_floats, usecols=data.columns[1:])
            .pipe(remove_missing_rows, usecols=data.columns[1:])
        ).reset_index(drop=True)

        # Check length
        assert len(data) == 11

        # Assign uniform sectors
        data["sector"] = SECTORS
        data["parent_sector"] = np.select(
            [
                data["sector"].isin(
                    [
                        "General Commercial",
                        "Office Buildings, Hotels and Garages",
                        "Industrial",
                        "Other Non-Residential",
                        "Total Non-Residential",
                    ]
                ),
                data["sector"].isin(
                    [
                        "Condos",
                        "Apartments",
                        "Single/Multi-family Homes",
                        "Total Residential",
                    ]
                ),
            ],
            ["Non-Residential", "Residential"],
            default="",
        )
        data["parent_sector"] = data["parent_sector"].replace("", np.nan)

        return data

    def validate(self, data):
        """Validate the input data."""

        sub = data.query("sector in ['Residential', 'Non-Residential', 'Unclassified']")
        total = data.query("sector == 'Total'")["total"].squeeze()

        diff = sub["total"].sum() - total
        assert diff < 5

        return True

    def load(self, data) -> None:
        """Load the data."""

        # Get the path
        dirname = self.get_data_directory("processed")
        path = dirname / f"{self.year}_{self.month:02d}.csv"

        # Load
        super()._load_csv_data(data, path)
