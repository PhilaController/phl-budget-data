"""Command-line interface."""

import click
from loguru import logger

from .etl import DATA_DIR, collections


@click.command()
@click.version_option()
@click.argument(
    "kind", type=click.Choice(["city-tax", "city-nontax", "city-other-govts", "wage"])
)
@click.option("--month", type=int)
@click.option("--year", type=int)
@click.option("--dry-run", is_flag=True)
def monthly_collections_etl(kind, month=None, year=None, dry_run=False) -> None:
    """Run the ETL pipeline for monthly collections"""

    # Get the ETL class
    ETL = {
        "wage": collections.WageCollectionsByIndustry,
        "city-tax": collections.CityTaxCollections,
        "city-nontax": collections.CityNonTaxCollections,
        "city-other-govts": collections.CityOtherGovtsCollections,
    }
    cls = ETL[kind]

    # Log
    logger.info(f"Processing ETL for '{cls.__name__}'")

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

            # Get month and year
            year, month = map(int, f.stem.split("_"))
            logger.info(f"Processing year='{year}' and month='{month}'")

            # ETL
            if not dry_run:
                report = cls(year=year, month=month)
                report.extract_transform_load()


@click.command()
@click.version_option()
@click.option("--fiscal-year", type=int)
@click.option("--dry-run", is_flag=True)
def sales_collections_etl(fiscal_year=None, dry_run=False) -> None:
    """Run the ETL pipeline for monthly sales collections"""

    # Get the ETL class
    cls = collections.SalesCollectionsByIndustry

    # Log
    logger.info(f"Processing ETL for '{cls.__name__}'")

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


@click.command()
@click.version_option()
@click.option("--dry-run", is_flag=True)
def birt_collections_etl(fiscal_year=None, dry_run=False) -> None:
    """Run the ETL pipeline for monthly BIRT collections"""

    # Get the ETL class
    cls = collections.BIRTCollectionsByIndustry

    # Log
    logger.info(f"Processing ETL for '{cls.__name__}'")

    # ETL
    if not dry_run:
        report = cls()
        report.extract_transform_load()
