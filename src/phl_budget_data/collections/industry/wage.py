import calendar
from dataclasses import dataclass
from operator import attrgetter

import pdfplumber

from ... import DATA_DIR
from ...base import ETLPipeline
from ...utils.pdf import extract_words, fuzzy_groupby
from ...utils.transformations import *

INDUSTRIES = [
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
class WageCollectionsByIndustry(ETLPipeline):
    """
    Monthly wage collections by industry.

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

    month: int
    year: int

    def __post_init__(self):
        """Set up necessary variables."""

        # The PDF path
        self.path = (
            self._get_data_directory("raw") / f"{self.year}_{self.month:02d}.pdf"
        )

        # Make sure this path exists
        if not self.path.exists():
            raise FileNotFoundError(
                f"No PDF available for month '{self.month}' and year '{self.year}'"
            )

        # Month name
        self.month_name = calendar.month_abbr[self.month].lower()

    def _get_data_directory(self, kind: str) -> str:
        """Internal function to get the file path.

        Parameters
        ----------
        kind : {'raw', 'processed'}
            type of data to load
        """
        assert kind in ["raw", "processed"]
        return DATA_DIR / kind / "collections" / "by-industry" / "wage-monthly"

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

        columns = ["industry"]

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
        data["industry"] = INDUSTRIES
        data["parent_industry"] = np.select(
            [
                data["industry"].isin(
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
                data["industry"].isin(
                    [
                        "Hospitals",
                        "Doctors, Dentists, and Other Health Practitioners",
                        "Outpatient Care Centers and Other Health Services",
                        "Nursing & Personal Care Facilities",
                        "Social Services",
                    ]
                ),
                data["industry"].isin(
                    [
                        "College and Universities",
                        "Elementary, Secondary Schools",
                        "Other Educational Services",
                    ]
                ),
                data["industry"].isin(
                    [
                        "Legal Services",
                        "Management Consulting",
                        "Engineering & Architectural Services",
                        "Computer",
                        "Accounting, Auditing, Bookkeeping",
                        "Advertising and Other Professional Services",
                    ]
                ),
                data["industry"].isin(
                    [
                        "Membership Organizations",
                        "Employment/Outsourcing Agencies",
                        "Security and Investigation Services",
                        "Services to Buildings",
                        "Miscellaneous Sectors",
                    ]
                ),
                data["industry"].isin(
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
        data["parent_industry"] = data["parent_industry"].replace("", np.nan)

        return data

    def validate(self, data):
        """Validate the input data."""

        cols = [f"{self.month_name}_{self.year-i}" for i in [0, 1, 2, 3]]

        # Sum up
        subsectors = data.query("parent_industry.notnull()")
        totals = subsectors.groupby("parent_industry")[cols].sum()

        # Compare to total
        for col in cols:
            for industry in totals.index:
                total1 = totals.loc[industry, col]
                total2 = data.loc[data["industry"] == industry][col].squeeze()
                diff = total1 - total2
                assert diff < 5

        return True

    def load(self, data) -> None:
        """Load the data."""

        path = (
            self._get_data_directory("processed") / f"{self.year}-{self.month:02d}.csv"
        )
        if not path.parent.exists():
            path.parent.mkdir(parents=True)

        # Save
        data.to_csv(path, index=False)

        return None
