from dataclasses import dataclass
from operator import attrgetter

import pdfplumber

from ... import DATA_DIR
from ...etl import ETLPipeline
from ...utils.pdf import extract_words, fuzzy_groupby
from ...utils.transformations import *

SECTORS = [
    "Construction",
    "Manufacturing",
    "Food and Beverage Products",
    "Chemicals, Pharmaceuticals & Petroleum",
    "Other Manufacturing",
    "Wholesale Trade",
    "Retail Trade",
    "Transportation and Storage",
    "Information",
    "Publishing",
    "Broadcasting (TV and Radio)",
    "Telecommunications",
    "Other Information",
    "Banking and Related Activities",
    "Financial Investment Services",
    "Insurance",
    "Real Estate",
    "Professional Services",
    "Legal Services",
    "Accounting, Tax and Payroll Services",
    "Architect and Engineering",
    "Computer Services",
    "Management and Technical Consulting",
    "Advertising",
    "Other Professional Services",
    "Business Support Services",
    "Educational Services",
    "Health and Social Services",
    "Sports",
    "Hotels and Other Accommodations",
    "Restaurants, Bars, and Other Food Services",
    "Other Personal Services",
    "All Other Sectors",
    "Unclassified",
]


@dataclass
class BIRTCollectionsBySector(ETLPipeline):
    """
    Tax year BIRT collections by sector.
    """

    def __post_init__(self):
        """Set up necessary variables."""

        # The PDF path
        self.path = self.get_data_directory("raw") / "tax-years-2004-2018.pdf"

    @classmethod
    def get_data_directory(cls, kind: str) -> str:
        """Internal function to get the file path.

        Parameters
        ----------
        kind : {'raw', 'processed'}
            type of data to load
        """
        assert kind in ["raw", "processed"]

        return DATA_DIR / kind / "collections" / "by-sector" / "birt"

    def extract(self) -> pd.DataFrame:
        """Extract the data from the first PDF page."""

        # Open the PDF document
        with pdfplumber.open(self.path) as pdf:

            out = []
            for pg in pdf.pages:

                df = pd.DataFrame(pg.extract_table())
                assert len(df) == 38

                # Do the first tax year
                X = df[[0, 1, 2, 3, 4]].iloc[3:]
                X.columns = [
                    "sector",
                    "num_accounts",
                    "net_income",
                    "gross_receipts",
                    "total",
                ]

                tax_year = df[1].str.extract("Tax Year (\d{4})").dropna().squeeze()
                X["tax_year"] = tax_year
                out.append(X)

                # Do the second tax year
                if len(df.columns) > 5:

                    X = df[[0, 5, 6, 7, 8]].iloc[3:]
                    X.columns = [
                        "sector",
                        "num_accounts",
                        "net_income",
                        "gross_receipts",
                        "total",
                    ]

                    tax_year = df[5].str.extract("Tax Year (\d{4})").dropna().squeeze()
                    X["tax_year"] = tax_year
                    out.append(X)

            return pd.concat(out, axis=0).reset_index(drop=True).fillna("")

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform the raw parsing data into a clean data frame."""

        # Apply set of base transformations first
        data = (
            data.pipe(remove_spaces).pipe(convert_to_floats, usecols=data.columns[1:])
        ).reset_index(drop=True)

        # Remove total
        data = data.query("sector != 'Total'").copy()

        # Set tax year as int
        data["tax_year"] = data["tax_year"].astype(int)

        # Assign uniform industries
        for tax_year in data["tax_year"].unique():
            sel = data["tax_year"] == tax_year
            data.loc[sel, "sector"] = SECTORS

        data["parent_sector"] = np.select(
            [
                data["sector"].isin(
                    [
                        "Food and Beverage Products",
                        "Chemicals, Pharmaceuticals & Petroleum",
                        "Other Manufacturing",
                    ]
                ),
                data["sector"].isin(
                    [
                        "Publishing",
                        "Broadcasting (TV and Radio)",
                        "Telecommunications",
                        "Other Information",
                    ]
                ),
                data["sector"].isin(
                    [
                        "Legal Services",
                        "Accounting, Tax and Payroll Services",
                        "Architect and Engineering",
                        "Computer Services",
                        "Management and Technical Consulting",
                        "Advertising",
                        "Other Professional Services",
                    ]
                ),
            ],
            ["Manufacturing", "Information", "Professional Services"],
            default="",
        )
        data["parent_sector"] = data["parent_sector"].replace("", np.nan)

        return data.sort_values("tax_year", ascending=False).reset_index(drop=True)

    def validate(self, data):
        """Validate the input data."""

        # Sub industries
        subsectors = data.query("parent_sector.notnull()")
        totals = subsectors.groupby(["tax_year", "parent_sector"])["total"].sum()

        # Compare to total
        for (tax_year, sector) in totals.index:
            total1 = totals.loc[(tax_year, sector)]
            total2 = data.query(f"sector == '{sector}' and tax_year == {tax_year}")[
                "total"
            ].squeeze()
            diff = total1 - total2
            assert diff < 5

        return True

    def load(self, data) -> None:
        """Load the data."""

        # Get the path
        path = self.get_data_directory("processed") / "tax-years-2004-2018.csv"

        # Load
        super()._load_csv_data(data, path)
