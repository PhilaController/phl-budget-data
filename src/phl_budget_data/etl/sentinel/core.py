import tempfile
from urllib.error import HTTPError

import click
from loguru import logger

from .. import DATA_DIR
from ..collections import *
from .scrape import *


def _get_latest_raw_pdf(cls):
    """Given an ETL class, return the latest PDF in the data directory."""

    # Get PDF paths
    dirname = cls.get_data_directory("raw")
    pdf_files = dirname.glob("*.pdf")

    # Get the latest
    latest = sorted(pdf_files)[-1]
    year, month = map(int, latest.stem.split("_"))

    # Next month and year
    if month == 12:
        next_month = 1
        next_year = year + 1
    else:
        next_month = month + 1
        next_year = year

    return next_year, next_month


def _run_monthly_update(month, year, url, css_identifier, *etls):

    # Try to extract out the PDF links from the page
    try:
        pdf_urls = extract_pdf_urls(url, css_identifier)
    except HTTPError as err:
        if err.code == 404:
            logger.info(f"URL '{url}' does not exist")
            return None
        else:
            raise

    # Download and run ETL
    if month in pdf_urls:

        # Download to temp dir initially
        with tempfile.TemporaryDirectory() as tmpdir:

            # Get the driver
            driver = get_driver(tmpdir)

            # The remote URL
            remote_pdf_path = pdf_urls[month]

            # Log
            logger.info(f"Downloading PDF from '{remote_pdf_path}'")

            # Local path
            dirname = etls[0].get_data_directory("raw")
            local_pdf_path = dirname / f"{year}_{month:02d}.pdf"

            # Download the PDF
            with downloaded_pdf(
                driver, remote_pdf_path, tmpdir, interval=1
            ) as pdf_path:

                if not local_pdf_path.parent.exists():
                    local_pdf_path.parent.mkdir()

                pdf_path.rename(local_pdf_path)

            # Run the ETL
            try:
                for cls in etls:

                    # Log
                    logger.info(f"Running ETL for {cls.__name__}")

                    # Run the ETL
                    report = cls(year=year, month=month)
                    report.extract_transform_load()
            except Exception:

                if local_pdf_path.exists():
                    local_pdf_path.unlink()
                raise

    else:
        logger.info(f"...no updates found")


@click.command()
@click.version_option()
def update_monthly_wage_collections():
    """Check for updates to the monthly wage collection report."""

    # Get the month/year of next PDF to look for
    year, month = _get_latest_raw_pdf(WageCollectionsBySector)

    # Log
    logger.info(f"Checking for PDF report for month '{month}' and year '{year}'")

    # Extract out PDF urls on the city's website
    url = f"https://www.phila.gov/documents/{year}-wage-tax-by-industry/"
    css_identifier = "wage-taxes"

    # Run the update
    _run_monthly_update(month, year, url, css_identifier, WageCollectionsBySector)


@click.command()
@click.version_option()
def update_monthly_city_collections():
    """Check for updates to the monthly city collection report."""

    # Get the month/year of next PDF to look for
    year, month = _get_latest_raw_pdf(CityTaxCollections)

    # Get the fiscal year
    if month < 7:
        fiscal_year = year
    else:
        fiscal_year = year + 1

    # Log
    logger.info(f"Checking for PDF report for month '{month}' and year '{year}'")

    # Extract out PDF urls on the city's website
    url = f"https://www.phila.gov/documents/fy-{fiscal_year}-city-monthly-revenue-collections/"
    css_identifier = "revenue-collections"

    # Run the update
    _run_monthly_update(
        month,
        year,
        url,
        css_identifier,
        CityTaxCollections,
        CityNonTaxCollections,
        CityOtherGovtsCollections,
    )


@click.command()
@click.version_option()
def update_monthly_school_collections():
    """Check for updates to the monthly school district collection report."""

    # Get the month/year of next PDF to look for
    year, month = _get_latest_raw_pdf(SchoolTaxCollections)

    # Get the fiscal year
    if month < 7:
        fiscal_year = year
    else:
        fiscal_year = year + 1

    # Log
    logger.info(f"Checking for PDF report for month '{month}' and year '{year}'")

    # Extract out PDF urls on the city's website
    url = f"https://www.phila.gov/documents/fy-{fiscal_year}-school-district-monthly-revenue-collections/"
    css_identifier = "revenue-collections"

    # Run the update
    _run_monthly_update(month, year, url, css_identifier, SchoolTaxCollections)
