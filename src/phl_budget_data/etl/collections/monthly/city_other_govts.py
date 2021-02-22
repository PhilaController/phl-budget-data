from .city import CityCollectionsReport
from .core import get_column_names


class CityOtherGovtsCollections(CityCollectionsReport):
    """Monthly City Other Governments Collections Report"""

    def transform(self, data):
        """Transform."""

        # Call base transform
        df = super().transform(data)

        # Get the start index
        start = df[0].str.contains("U.S. GOV", regex=True)
        start = start[start]
        assert len(start) == 1
        start = start.index[0]

        # Get the stop index
        stop = df[0].str.contains("TOTAL.*REVENUE.*GOV.*", regex=True)
        stop = stop[stop]
        assert len(stop) == 1
        stop = stop.index[0]

        # Trim
        df = df.loc[start:stop]

        # Check the length
        assert len(df) in [5, 7], f"length validation failed; length = {len(df)}"

        # Format the first column of names
        df[0] = (
            df[0]
            .str.strip()
            .str.lower()
            .str.replace("-", " ", regex=False)
            .str.replace(".", "", regex=False)
            .str.replace("&", "and", regex=False)
            .str.replace("\s+", "_", regex=True)
        )

        # Fix total
        sel = df[0].str.startswith("total_revenue_from")
        df.loc[sel, 0] = "total_revenue_other_govts"

        # Fix authorized adjustment
        sel = df[0].str.startswith("other_authorized")
        df.loc[sel, 0] = "other_authorized_adjustment"

        # Set the columns
        columns = get_column_names(self.month, self.year)
        columns = ["name"] + columns[-7:]
        df = df[[0] + list(df.columns[-7:])]
        df.columns = columns

        return df

    def load(self, data):
        """Load the data."""

        # Get the processed data path
        dirname = self.get_data_directory("processed")
        path = dirname / f"{self.year}-{self.month:02d}-other-govts.csv"

        # Load
        super()._load_csv_data(data, path)

    def validate(self, data):
        """Validate the input data."""

        # Trim to the month columns
        data = data.filter(regex=f"^{self.month_name}|name", axis=1)

        # Compare
        subcategories = data.query("name != 'total_revenue_other_govts'")
        total = data.query("name == 'total_revenue_other_govts'").squeeze()

        for col in data.columns:
            if col == "name":
                continue
            diff = subcategories[col].sum() - total[col]
            assert diff < 5

        return True
