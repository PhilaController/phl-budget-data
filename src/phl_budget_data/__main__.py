"""Command-line interface."""

import click
from loguru import logger

from .etl import DATA_DIR, collections


def _fiscal_year_etl(cls, fiscal_year, dry_run):
    """Internal function to run ETL on fiscal year data."""

    # Get the directory of raw files
    dirname = cls.get_data_directory("raw")

    # Glob the PDF files
    if fiscal_year is not None:
        fy_tag = str(fiscal_year)[-2:]
        files = dirname.glob(f"FY{fy_tag}.pdf")
    else:
        files = dirname.glob("*.pdf")

    # Do all the files
    for f in sorted(files):

        # Get fiscal_year
        fiscal_year = int(f"20{f.stem[2:]}")
        logger.info(f"Processing fiscal_year='{fiscal_year}'")

        # ETL
        if not dry_run:
            report = cls(fiscal_year=fiscal_year)
            report.extract_transform_load()


def _monthly_etl(cls, month, year, dry_run):
    """Internal function to run ETL on monthly data."""

    # If month is provided, we need the year too
    if month is not None:
        if year is None:
            raise ValueError("Year is required if month is provided")

    # Do a single month and year
    if month is not None:

        # Log
        logger.info(f"Processing year='{year}' and month='{month}'")

        # Do the ETL
        if not dry_run:
            report = cls(year=year, month=month)
            report.extract_transform_load()
    else:

        # Get the directory of raw files
        dirname = cls.get_data_directory("raw")

        # Glob the PDF files
        if year is not None:
            files = dirname.glob(f"{year}*.pdf")
        else:
            files = dirname.glob("*.pdf")

        # Do all the files
        for f in sorted(files):

            # Get month, year
            if "Q" in f.stem:
                year, quarter = f.stem.split("_")
                quarter = int(quarter[1:])
                months = {1: [1, 2, 3], 2: [4, 5, 6], 3: [7, 8, 9], 4: [10, 11, 12]}
                months = months[quarter]
            else:
                year, month = map(int, f.stem.split("_"))
                months = [month]

            # Run each month
            for month in months:
                logger.info(f"Processing year='{year}' and month='{month}'")

                # ETL
                if not dry_run:
                    report = cls(year=int(year), month=int(month))
                    report.extract_transform_load()


@click.command()
@click.version_option()
@click.argument(
    "kind",
    type=click.Choice(["city-tax", "city-nontax", "city-other-govts", "school-tax"]),
)
@click.option("--month", type=int)
@click.option("--year", type=int)
@click.option("--dry-run", is_flag=True)
def etl_monthly_collections(kind, month=None, year=None, dry_run=False) -> None:
    """Run the ETL pipeline for monthly collections"""

    # Get the ETL class
    ETL = {
        "city-tax": collections.CityTaxCollections,
        "city-nontax": collections.CityNonTaxCollections,
        "city-other-govts": collections.CityOtherGovtsCollections,
        "school-tax": collections.SchoolTaxCollections,
    }
    cls = ETL[kind]

    # Log
    logger.info(f"Processing ETL for '{cls.__name__}'")

    # Run the ETL
    _monthly_etl(cls, month, year, dry_run)


@click.command()
@click.version_option()
@click.argument(
    "kind",
    type=click.Choice(["wage", "rtt", "sales", "birt"]),
)
@click.option("--month", type=int)
@click.option("--year", type=int)
@click.option("--dry-run", is_flag=True)
def etl_sector_collections(
    kind, month=None, year=None, fiscal_year=None, dry_run=False
) -> None:
    """Run the ETL pipeline for sector collections"""

    # Get the ETL class
    ETL = {
        "wage": collections.WageCollectionsBySector,
        "sales": collections.SalesCollectionsBySector,
        "rtt": collections.RTTCollectionsBySector,
        "birt": collections.BIRTCollectionsBySector,
    }
    cls = ETL[kind]

    # Log
    logger.info(f"Processing ETL for '{cls.__name__}'")

    # BIRT
    if kind == "birt":
        # ETL
        if not dry_run:
            report = cls()
            report.extract_transform_load()
    # Wage
    elif kind == "wage":
        _monthly_etl(cls, month, year, dry_run)
    # Sales
    elif kind == "sales":
        _fiscal_year_etl(cls, fiscal_year, dry_run)
    # RTT
    elif kind == "rtt":
        _monthly_etl(cls, month, year, dry_run)
