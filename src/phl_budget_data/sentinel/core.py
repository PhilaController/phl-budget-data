import tempfile
from urllib.error import HTTPError

import click
from loguru import logger

from .. import DATA_DIR
from ..collections import *
from .scrape import *


@click.command()
@click.version_option()
def update_monthly_wage_collections():
    """Check for updates to the monthly wage collection report."""

    # Get latest PDF
    dirname = DATA_DIR / "raw" / "collections" / "by-industry" / "wage-monthly"
    latest = sorted(dirname.glob("*.pdf"))[-1]
    year, month = map(int, latest.stem.split("_"))

    # Next month and year
    if month == 12:
        next_month = 1
        next_year = year + 1
    else:
        next_month = month + 1
        next_year = year

    # Log
    logger.info(
        f"Checking for PDF report for month '{next_month}' and year '{next_year}'"
    )

    # Extract out PDF urls on the city's website
    url = f"https://www.phila.gov/documents/{next_year}-wage-tax-by-industry/"

    try:
        pdf_urls = extract_pdf_urls(url, "wage-taxes")
    except HTTPError as err:
        if err.code == 404:
            logger.info(f"URL '{url}' does not exist")
            return None
        else:
            raise

    # Download and run ETL
    if next_month in pdf_urls:

        # Download to temp dir initially
        with tempfile.TemporaryDirectory() as tmpdir:

            # Get the driver
            driver = get_driver(tmpdir)

            # The remote URL
            remote_pdf_path = pdf_urls[next_month]

            # Log
            logger.info(f"Downloading PDF from '{remote_pdf_path}'")

            # Local path
            local_pdf_path = dirname / f"{next_year}_{next_month:02d}.pdf"

            # Download the PDF
            with downloaded_pdf(
                driver, remote_pdf_path, tmpdir, interval=1
            ) as pdf_path:

                if not local_pdf_path.parent.exists():
                    local_pdf_path.parent.mkdir()

                pdf_path.rename(local_pdf_path)

            try:
                # Log
                logger.info("Running ETL for WageCollectionsByIndustry")

                # Run the ETL
                report = WageCollectionsByIndustry(year=next_year, month=next_month)
                report.extract_transform_load()
            except Exception:

                if local_pdf_path.exists():
                    local_pdf_path.unlink()
                raise

    else:
        logger.info(f"...no updates found")


@click.command()
@click.version_option()
def update_monthly_city_collections():
    """Check for updates to the monthly city collection report."""

    # Get latest PDF
    dirname = DATA_DIR / "raw" / "collections" / "city-monthly"
    latest = sorted(dirname.glob("*.pdf"))[-1]
    year, month = map(int, latest.stem.split("_"))

    # Next month and year
    if month == 12:
        next_month = 1
        next_year = year + 1
    else:
        next_month = month + 1
        next_year = year

    # Get the fiscal year
    if month < 7:
        fiscal_year = year
    else:
        fiscal_year = year + 1

    # Log
    logger.info(
        f"Checking for PDF report for month '{next_month}' and year '{next_year}'"
    )

    # Extract out PDF urls on the city's website
    url = f"https://www.phila.gov/documents/fy-{fiscal_year}-city-monthly-revenue-collections/"
    try:
        pdf_urls = extract_pdf_urls(url, "revenue-collections")
    except HTTPError as err:
        if err.code == 404:
            logger.info(f"URL '{url}' does not exist")
            return None
        else:
            raise

    # Download and run ETL
    if next_month in pdf_urls:

        # Download to temp dir initially
        with tempfile.TemporaryDirectory() as tmpdir:

            # Get the driver
            driver = get_driver(tmpdir)

            # The remote URL
            remote_pdf_path = pdf_urls[next_month]

            # Log
            logger.info(f"Downloading PDF from '{remote_pdf_path}'")

            # Local path
            local_pdf_path = dirname / f"{next_year}_{next_month:02d}.pdf"

            # Download the PDF
            with downloaded_pdf(
                driver, remote_pdf_path, tmpdir, interval=1
            ) as pdf_path:

                if not local_pdf_path.parent.exists():
                    local_pdf_path.parent.mkdir()

                pdf_path.rename(local_pdf_path)

            # Run the ETL
            try:
                for cls in [
                    CityTaxCollections,
                    CityNonTaxCollections,
                    CityOtherGovtsCollections,
                ]:

                    # Log
                    logger.info(f"Running ETL for {cls.__name__}")

                    # Run the ETL
                    report = cls(year=next_year, month=next_month)
                    report.extract_transform_load()
            except Exception:

                if local_pdf_path.exists():
                    local_pdf_path.unlink()
                raise

    else:
        logger.info(f"...no updates found")
