from dataclasses import dataclass
from operator import attrgetter

import pdfplumber

from ... import DATA_DIR
from ...etl import ETLPipeline
from ...utils.pdf import extract_words, fuzzy_groupby
from ...utils.transformations import *

SECTORS = [
    "All Other Sectors",
    "Appliance, other electronics, retail",
    "Car and truck rental",
    "Computer and software stores, retail",
    "Construction",
    "Convenience stores, retail",
    "Department stores, retail",
    "Furniture stores retail",
    "Home centers, retail",
    "Hotels",
    "Liquor and beer stores, retail",
    "Manufacturing",
    "Motor Vehicle Sales Tax",
    "Office supplies stores, retail",
    "Other retail",
    "Pharmacies, retail",
    "Public Utilities",
    "Rentals except car and truck rentals",
    "Repair services",
    "Restaurants, bars, concessionaires and caterers",
    "Services other than repair services",
    "Subtotal",
    "Supermarkets, retail",
    "Telecommunications",
    "Total Retail",
    "Unclassified",
    "Wholesale",
]


@dataclass
class SalesCollectionsBySector(ETLPipeline):
    """
    Fiscal year sales collections by sector.

    Parameters
    ----------
    fiscal_year :
        the fiscal year; data is annual
    """

    fiscal_year: int

    def __post_init__(self):
        """Set up necessary variables."""

        # The PDF path
        fy_tag = str(self.fiscal_year)[-2:]
        self.path = self.get_data_directory("raw") / f"FY{fy_tag}.pdf"

        # Make sure this path exists
        if not self.path.exists():
            raise FileNotFoundError(
                f"No PDF available for fiscal year '{self.fiscal_year}'"
            )

        # Which file format?
        self.legacy = self.fiscal_year < 2017

    @classmethod
    def get_data_directory(cls, kind: str) -> str:
        """Internal function to get the file path.

        Parameters
        ----------
        kind : {'raw', 'processed'}
            type of data to load
        """
        assert kind in ["raw", "processed"]

        return DATA_DIR / kind / "collections" / "by-sector" / "sales"

    def extract(self) -> pd.DataFrame:
        """Extract the data from the first PDF page."""

        # Open the PDF document
        with pdfplumber.open(self.path) as pdf:

            # Only need first page
            pg = pdf.pages[0]

            # Determine crop areas
            all_words = extract_words(
                pg, keep_blank_chars=True, x_tolerance=2, y_tolerance=1
            )

            ## TOP LEFT
            top_left = [
                w
                for w in all_words
                if w.text.strip().lower().startswith("construction")
            ]
            top_left = min(top_left, key=lambda w: w.x0)

            ## BOTTOM LEFT
            bottom_left = [
                w for w in all_words if w.text.strip().lower().startswith("motor")
            ]
            bottom_left = min(bottom_left, key=lambda w: w.x0)

            # Crop the main part of the document and extract the words
            cropped = pg.crop(
                [pg.bbox[0], top_left.top, pg.bbox[2], bottom_left.bottom + 3]
            )

            # Table strategy based on format
            if self.legacy:
                horizontal_strategy = "lines"
            else:
                horizontal_strategy = "text"

            return pd.DataFrame(
                cropped.extract_table(
                    {
                        "vertical_strategy": "lines",
                        "horizontal_strategy": horizontal_strategy,
                    }
                ),
            )

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform the raw parsing data into a clean data frame."""

        # Apply set of base transformations first
        data = (
            data.pipe(remove_spaces)
            .pipe(fix_percentages)
            .pipe(strip_dollar_signs)
            .pipe(replace_missing_cells)
            .pipe(convert_to_floats, usecols=data.columns[1:])
        ).reset_index(drop=True)

        # Check length
        assert len(data) == 27

        if not self.legacy:
            assert len(data.columns) == 12
            data = data[[0, 1, 2, 3]]
        else:
            assert len(data.columns) == 4

        # Set the columns
        data.columns = ["sector", "number_entities", "total", "percent_of_total"]

        # Sort by first column
        data = data.sort_values("sector")

        # Assign uniform industries
        data["sector"] = SECTORS
        data["parent_sector"] = np.select(
            [
                data["sector"].isin(
                    [
                        "Furniture stores retail",
                        "Appliance, other electronics, retail",
                        "Computer and software stores, retail",
                        "Home centers, retail",
                        "Supermarkets, retail",
                        "Convenience stores, retail",
                        "Liquor and beer stores, retail",
                        "Pharmacies, retail",
                        "Department stores, retail",
                        "Office supplies stores, retail",
                        "Other retail",
                    ]
                ),
            ],
            [
                "Total Retail",
            ],
            default="",
        )
        data["parent_sector"] = data["parent_sector"].replace("", np.nan)

        return data.sort_index()

    def validate(self, data):
        """Validate the input data."""

        # Sum up
        main_industries = data.query(
            "parent_sector.isnull() and sector != 'Subtotal' and sector != 'Motor Vehicle Sales Tax'"
        )
        subtotal1 = main_industries["total"].sum()
        subtotal2 = data.query("sector == 'Subtotal'")["total"].squeeze()
        diff = subtotal1 - subtotal2
        assert diff < 5

        # Sub industries
        subsectors = data.query("parent_sector.notnull()")
        totals = subsectors.groupby("parent_sector")["total"].sum()

        # Compare to total
        for sector in totals.index:
            total1 = totals.loc[sector]
            total2 = data.loc[data["sector"] == sector]["total"].squeeze()
            diff = total1 - total2
            assert diff < 5

        return True

    def load(self, data) -> None:
        """Load the data."""

        # Get the path
        fy_tag = str(self.fiscal_year)[-2:]
        path = self.get_data_directory("processed") / f"FY{fy_tag}.csv"

        # Load
        super()._load_csv_data(data, path)
