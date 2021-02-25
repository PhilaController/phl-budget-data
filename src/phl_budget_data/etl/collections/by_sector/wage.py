import calendar
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
    "Chemicals, Petroleum Refining",
    "Pharmaceuticals",
    "Transportation Equipment",
    "Food & Beverage Products",
    "Machinery, Electronic, and Other Electric Equipment",
    "Metal Manufacturing",
    "Miscellaneous Manufacturing",
    "Public Utilities",
    "Transportation and Warehousing",
    "Telecommunication",
    "Publishing, Broadcasting, and Other Information",
    "Wholesale Trade",
    "Retail Trade",
    "Banking & Credit Unions",
    "Securities / Financial Investments",
    "Insurance",
    "Real Estate, Rental and Leasing",
    "Health and Social Services",
    "Hospitals",
    "Doctors, Dentists, and Other Health Practitioners",
    "Outpatient Care Centers and Other Health Services",
    "Nursing & Personal Care Facilities",
    "Social Services",
    "Education",
    "College and Universities",
    "Elementary, Secondary Schools",
    "Other Educational Services",
    "Professional Services",
    "Legal Services",
    "Management Consulting",
    "Engineering & Architectural Services",
    "Computer",
    "Accounting, Auditing, Bookkeeping",
    "Advertising and Other Professional Services",
    "Hotels",
    "Restaurants",
    "Sport Teams",
    "Arts, Entertainment, and Other Recreation",
    "Other Sectors",
    "Membership Organizations",
    "Employment/Outsourcing Agencies",
    "Security and Investigation Services",
    "Services to Buildings",
    "Miscellaneous Sectors",
    "Government",
    "State Government (PA)",
    "City, School District, Local Quasi Govt.",
    "Federal Government",
    "Other Governments",
    "Unclassified Accounts",
]


@dataclass
class WageCollectionsBySector(ETLPipeline):
    """
    Monthly wage collections by sector.

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

        # The PDF path
        self.path = self.get_data_directory("raw") / f"{self.year}_{self.month:02d}.pdf"

        # Make sure this path exists
        if not self.path.exists():
            raise FileNotFoundError(
                f"No PDF available for month '{self.month}' and year '{self.year}'"
            )

        # Month name
        self.month_name = calendar.month_abbr[self.month].lower()

    @classmethod
    def get_data_directory(cls, kind: str) -> str:
        """Internal function to get the file path.

        Parameters
        ----------
        kind : {'raw', 'processed'}
            type of data to load
        """
        assert kind in ["raw", "processed"]
        return DATA_DIR / kind / "collections" / "by-sector" / "wage"

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
                w
                for w in all_words
                if w.text.strip().lower().startswith("unclassified")
            ]
            bottom_left = min(bottom_left, key=lambda w: w.x0)

            # Crop the main part of the document and extract the words
            cropped = pg.crop(
                [top_left.x0, top_left.top, pg.bbox[2], bottom_left.bottom]
            )
            words = extract_words(
                cropped, keep_blank_chars=True, x_tolerance=2, y_tolerance=1
            )

            # Group into rows
            d = []
            for k, v in fuzzy_groupby(
                words, key="bottom", lower_tol=3, upper_tol=3
            ).items():
                d.append([w.text for w in sorted(v, key=attrgetter("x0"))])

            return pd.DataFrame(d)

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform the raw parsing data into a clean data frame."""

        # Apply set of base transformations first
        data = (
            data.pipe(remove_spaces)
            .pipe(fix_percentages)
            .pipe(replace_missing_cells)
            .pipe(convert_to_floats, usecols=data.columns[1:])
        ).reset_index(drop=True)

        # Check length
        assert len(data) == 52
        assert len(data.columns) == 10

        columns = ["sector"]

        # Monthly collections over time
        for i in [0, 1, 2, 3]:
            columns.append(f"{self.month_name}_{self.year-i}")

        # Growth rates
        for i in [0, 1, 2]:
            columns.append(f"growth_yoy_{self.year-i}")
        columns += ["growth_3yr", "net_change"]

        # Set the columns
        data.columns = columns

        # Assign uniform industries
        data["sector"] = SECTORS
        data["parent_sector"] = np.select(
            [
                data["sector"].isin(
                    [
                        "Chemicals, Petroleum Refining",
                        "Pharmaceuticals",
                        "Transportation Equipment",
                        "Food & Beverage Products",
                        "Machinery, Electronic, and Other Electric Equipment",
                        "Metal Manufacturing",
                        "Miscellaneous Manufacturing",
                    ]
                ),
                data["sector"].isin(
                    [
                        "Hospitals",
                        "Doctors, Dentists, and Other Health Practitioners",
                        "Outpatient Care Centers and Other Health Services",
                        "Nursing & Personal Care Facilities",
                        "Social Services",
                    ]
                ),
                data["sector"].isin(
                    [
                        "College and Universities",
                        "Elementary, Secondary Schools",
                        "Other Educational Services",
                    ]
                ),
                data["sector"].isin(
                    [
                        "Legal Services",
                        "Management Consulting",
                        "Engineering & Architectural Services",
                        "Computer",
                        "Accounting, Auditing, Bookkeeping",
                        "Advertising and Other Professional Services",
                    ]
                ),
                data["sector"].isin(
                    [
                        "Membership Organizations",
                        "Employment/Outsourcing Agencies",
                        "Security and Investigation Services",
                        "Services to Buildings",
                        "Miscellaneous Sectors",
                    ]
                ),
                data["sector"].isin(
                    [
                        "State Government (PA)",
                        "City, School District, Local Quasi Govt.",
                        "Federal Government",
                        "Other Governments",
                    ]
                ),
            ],
            [
                "Manufacturing",
                "Health and Social Services",
                "Education",
                "Professional Services",
                "Other Sectors",
                "Government",
            ],
            default="",
        )
        data["parent_sector"] = data["parent_sector"].replace("", np.nan)

        return data

    def validate(self, data):
        """Validate the input data."""

        cols = [f"{self.month_name}_{self.year-i}" for i in [0, 1, 2, 3]]

        # Sum up
        subsectors = data.query("parent_sector.notnull()")
        totals = subsectors.groupby("parent_sector")[cols].sum()

        # Compare to total
        for col in cols:
            for sector in totals.index:
                total1 = totals.loc[sector, col]
                total2 = data.loc[data["sector"] == sector][col].squeeze()
                diff = total1 - total2
                assert diff < 5

        return True

    def load(self, data) -> None:
        """Load the data."""

        # Path to save data to
        dirname = self.get_data_directory("processed")
        path = dirname / f"{self.year}-{self.month:02d}.csv"

        # Load
        super()._load_csv_data(data, path)
