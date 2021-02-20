import numpy as np

from ... import DATA_DIR
from ...utils.misc import rename_tax_rows
from ...utils.transformations import remove_missing_rows
from .core import CityCollectionsReport, get_collections_report_columns


class CityTaxCollections(CityCollectionsReport):
    """Monthly City Tax Collections Report"""

    def transform(self, data):
        """Transform."""

        # Call base transform
        data = super().transform(data)

        # Split out just tax part
        cutoff = data.index[data[0].str.contains("TOTAL TAX REVENUE") == True]
        assert len(cutoff) == 1

        # Trim
        tax = data.loc[: cutoff[0]].copy().dropna(how="all", axis=1)

        # Remove Data Warehouse
        sel = tax[0].str.contains("DATA WAREHOUSE")
        if sel.sum():
            tax = tax.loc[~sel]
            tax = tax.reset_index(drop=True)

        # Determine columns for the report
        columns = get_collections_report_columns(self.month, self.year)
        columns = ["name"] + columns[-7:]
        tax = tax[[0] + list(tax.columns[-7:])]

        # Remove any empty rows
        tax = tax.pipe(remove_missing_rows, usecols=tax.columns[1:]).reset_index(
            drop=True
        )

        assert len(columns) == len(tax.columns)
        assert len(tax) in [39, 40, 42], len(tax)

        # Real estate + wage
        index = rename_tax_rows(
            tax,
            0,
            [
                "real_estate",
                "wage_city",
                "wage_pica",
            ],
        )
        tax.loc[index, 0] = "wage_total"
        index += 1

        # Earnings
        index = rename_tax_rows(
            tax,
            index,
            [
                "earnings_city",
                "earnings_pica",
            ],
        )
        tax.loc[index, 0] = "earnings_total"
        index += 1

        # Net Profits
        index = rename_tax_rows(
            tax,
            index,
            ["net_profits_city", "net_profits_pica"],
        )
        tax.loc[index, 0] = "net_profits_total"
        index += 1

        # Combo of wage, earnings, and NPT
        for i, suffix in enumerate(["", "pica", "city"]):

            if suffix:
                suffix = f"{suffix}_total"
            else:
                suffix = "total"
            tax.loc[index + i, 0] = f"wage_earnings_net_profits_{suffix}"
        index += 3

        # Add birt
        index = rename_tax_rows(
            tax,
            index,
            ["birt"],
        )

        # Other taxes
        other_taxes = [
            name + "_total"
            for name in [
                "sales",
                "amusement",
                "tobacco",
                "parking",
                "valet",
                "real_estate_transfer",
                "outdoor_ads",
            ]
        ]
        tax.loc[index : index + len(other_taxes) - 1, 0] = other_taxes
        index = index + len(other_taxes)

        # Handle remaining rows
        if len(tax) == 42:
            remaining = [
                "soda_current",
                "soda_prior",
                "soda_total",
                "other_taxes_total",
                "all_taxes_total",
            ]
        elif len(tax) == 40:
            remaining = ["soda_total", "other_taxes_total", "all_taxes_total"]
        elif len(tax):
            remaining = ["other_taxes_total", "all_taxes_total"]

        n = len(remaining)
        tax.loc[index : index + n - 1, 0] = remaining

        # Rename the columns
        tax.columns = columns

        # Split out current/prior/total into its own column
        tax["kind"] = tax["name"].apply(lambda x: x.split("_")[-1])
        tax["name"] = tax["name"].apply(lambda x: "_".join(x.split("_")[:-1]))

        return tax

    def load(self, data):
        """Load the data."""

        # Get the processed data path
        dirname = self._get_data_directory("processed")
        path = dirname / f"{self.year}-{self.month:02d}-tax.csv"

        if not path.parent.exists():
            path.parent.mkdir(parents=True)

        data.to_csv(path, index=False)

    def validate(self, data):
        """Validate the input data."""

        taxes = [
            "real_estate",
            "wage_city",
            "earnings_city",
            "net_profits_city",
            "birt",
            "sales",
            "amusement",
            "tobacco",
            "parking",
            "valet",
            "real_estate_transfer",
            "outdoor_ads",
            "soda",
            "other_taxes",
        ]
        t = data.query("kind == 'total' and name in @taxes")
        t = t.filter(regex=f"^{self.month_name}", axis=1)

        for col in t.columns:
            all_taxes = data.query("name == 'all_taxes'")[col].squeeze()
            diff = t[col].sum() - all_taxes
            assert diff < 5

        return True
