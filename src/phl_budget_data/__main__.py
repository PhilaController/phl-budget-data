"""Command-line interface."""
import click
from loguru import logger

from . import DATA_DIR, collections


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

    dirname = DATA_DIR / "raw" / "collections"
    if kind == "wage":
        cls = collections.WageCollectionsByIndustry
        dirname = dirname / "by-industry" / "wage"
    elif kind == "city-tax":
        cls = collections.CityTaxCollections
        dirname = dirname / "city-monthly"
    elif kind == "city-nontax":
        cls = collections.CityNonTaxCollections
        dirname = dirname / "city-monthly"
    elif kind == "city-other-govts":
        cls = collections.CityOtherGovtsCollections
        dirname = dirname / "city-monthly"

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
