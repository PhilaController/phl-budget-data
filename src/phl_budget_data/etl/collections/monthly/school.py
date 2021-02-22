import pandas as pd
import pdfplumber

from ... import DATA_DIR
from ...utils.misc import rename_tax_rows
from ...utils.pdf import extract_words, words_to_table
from .core import MonthlyCollectionsReport, get_column_names


class SchoolTaxCollections(MonthlyCollectionsReport):
    """
    Monthly School District Collections Report.

    Parameters
    ----------
    month :
        the calendar month number (starting at 1)
    year :
        the calendar year
    """

    report_type = "school"

    @property
    def legacy(self) -> bool:
        """Whether the format is the legacy or current version."""
        return self.num_pages > 1

    def extract(self) -> pd.DataFrame:
        """Internal function to parse the contents of a legacy PDF page."""

        # Open the PDF document
        with pdfplumber.open(self.path) as pdf:

            # Loop over each page
            out = []
            for pg in pdf.pages:

                # Extract the words
                words = extract_words(
                    pg, keep_blank_chars=False, x_tolerance=1, y_tolerance=1
                )

                # Group the words into a table
                data = words_to_table(
                    words,
                    text_tolerance_y=5,
                    text_tolerance_x=5,
                    column_tolerance=20,
                    min_col_sep=24,
                    header_column_overlap=10,
                )

                # Skip the header (first five rows)
                data = data.iloc[6:]
                assert "REAL ESTATE" in data.iloc[0][0]

                # # Remove first row of header if we need to
                # for phrase in ["prelim", "final", "budget"]:
                #     sel = data[0].str.lower().str.startswith(phrase)
                #     data = data.loc[~sel]

                # # Remove empty columns
                # data = remove_empty_columns(data, use_nan=False)

                # Check number of columns
                if len(out):
                    if len(data.columns) != len(out[-1].columns):
                        raise ValueError("Column mismatch when parsing multiple pages")

                # Save it
                out.append(data)

            # Return concatenation
            return pd.concat(out, axis=0, ignore_index=True)

    def transform(self, data):
        """Transform the raw parsing data into a clean data frame."""

        # Call base transform
        data = super().transform(data)

        # Determine columns for the report
        columns = get_column_names(self.month, self.year)

        ncols = len(data.columns)
        assert ncols in [11, 12, 14]

        if ncols == 14:
            data = data.drop(labels=[7, 8, 9, 10], axis=1)
        else:
            data = data.drop(labels=[data.columns[-6]], axis=1)

        # Set the columns
        columns = ["name"] + columns[-7:]
        data = data[[0] + list(data.columns[-7:])]

        assert len(columns) == len(data.columns)
        assert len(data) in [14, 15]

        # Do current/prior/total
        if len(data) == 14:
            index = rename_tax_rows(
                data,
                0,
                ["real_estate", "school_income", "use_and_occupancy", "liquor"],
            )
        else:
            index = rename_tax_rows(
                data,
                0,
                ["real_estate"],  # , "school_income", "use_and_occupancy", "liquor"],
            )

            if "PAYMENT" in data.loc[index, 0]:
                data.loc[index, 0] = "pilots_total"
                index += 1

            index = rename_tax_rows(
                data,
                index,
                ["school_income", "use_and_occupancy", "liquor"],
            )

            if "PAYMENT" in data.loc[index, 0]:
                data.loc[index, 0] = "pilots_total"
                index += 1

        # Other non-tax
        data.loc[index, 0] = "other_nontax_total"
        index += 1

        # Total
        data.loc[index, 0] = "total_revenue_total"
        index += 1

        # Set the columns
        data.columns = columns

        # Split out current/prior/total into its own column
        data["kind"] = data["name"].apply(lambda x: x.split("_")[-1])
        data["name"] = data["name"].apply(lambda x: "_".join(x.split("_")[:-1]))

        return data

    def validate(self, data):
        """Validate the input data."""

        # Sum up
        t = data.query("kind == 'total' and name != 'total_revenue'")
        t = t.filter(regex=f"^{self.month_name}", axis=1)

        # Compare to total
        for col in t.columns:
            total_revenue = data.query("name == 'total_revenue'")[col].squeeze()
            diff = t[col].sum() - total_revenue
            assert diff < 5

        return True

    def load(self, data) -> None:
        """Load the data."""

        # Get the path
        dirname = self.get_data_directory("processed")
        path = dirname / f"{self.year}-{self.month:02d}-tax.csv"

        # Load
        super()._load_csv_data(data, path)
