from .city import CityCollectionsReport
from .core import get_column_names


class CityNonTaxCollections(CityCollectionsReport):
    """Monthly City Non-Tax Collections Report"""

    def transform(self, data):
        """Transform."""

        # Call base transform
        df = super().transform(data)

        # Get the start index
        start = df[0].str.contains("TOTAL TAX REVENUE")
        start = start[start]
        assert len(start) == 1
        start = start.index[0]

        # Get the stop index
        stop = df[0].str.contains("TOTAL LOCAL NON.*TAX", regex=True)
        stop = stop[stop]
        assert len(stop) == 1
        stop = stop.index[0]

        # Trim
        df = df.loc[start:stop].iloc[1:]

        # Check the length
        assert len(df) in [10, 11, 17]
        if len(df) == 11:  # extra first row
            df = df.iloc[1:]

        # Format the first column of names
        df[0] = (
            df[0]
            .str.strip()
            .str.lower()
            .str.replace("-", "")
            .str.replace("&", "and")
            .str.replace("\s+", "_", regex=True)
        )

        # Fix Total local nontax revenue
        sel = df[0].str.startswith("total_local_non")
        df.loc[sel, 0] = "total_local_nontax_revenue"

        # Fix EMS
        sel = df[0].str.startswith("emergency_medical")
        df.loc[sel, 0] = "emergency_medical_services"

        # Rename
        df[0] = df[0].replace(
            {
                "licenses_and_inspections": "licenses_and_inspection_fees",
                "nonprofit_contribution": "payments_in_lieu_of_taxes",
                "interest_income": "interest_earnings",
                "sale_of_assets": "asset_sales",
                "court_related": "court_related_costs",
            }
        )

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
        path = dirname / f"{self.year}-{self.month:02d}-nontax.csv"

        # Load
        super()._load_csv_data(data, path)

    def validate(self, data):
        """Validate the input data."""

        # Trim to the month columns
        data = data.filter(regex=f"^{self.month_name}|name", axis=1)

        # Compare
        subcategories = data.query("name != 'total_local_nontax_revenue'")
        total = data.query("name == 'total_local_nontax_revenue'").squeeze()

        for col in data.columns:
            if col == "name":
                continue
            diff = subcategories[col].sum() - total[col]
            assert diff < 5

        return True
