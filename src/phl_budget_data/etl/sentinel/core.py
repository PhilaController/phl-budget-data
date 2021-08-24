import tempfile
from urllib.error import HTTPError

import click
import pandas as pd
from loguru import logger

from .. import DATA_DIR
from ..collections import *
from .scrape import *


def _to_fiscal_month(month):
    out = (month + 6) % 12
    if out == 0:
        out = 1
    return out


def _get_latest_raw_pdf(cls):
    """Given an ETL class, return the latest PDF in the data directory."""

    # Get PDF paths
    dirname = cls.get_data_directory("raw")
    pdf_files = dirname.glob("*.pdf")

    # Get the latest
    latest = sorted(pdf_files)[-1]
    year, month = map(int, latest.stem.split("_"))

    return year, month


def _run_monthly_update(month, year, url, css_identifier, *etls):
    """Internal function to run update on monthly PDFs."""

    # Try to extract out the PDF links from the page
    try:
        pdf_urls = extract_pdf_urls(url, css_identifier)
    except HTTPError as err:
        if err.code == 404:
            logger.info(f"URL '{url}' does not exist")
            return None
        else:
            raise

    # Find out which ones are new
    last_dt = pd.to_datetime(f"{month}/{year}")
    new_months = [dt for dt in pdf_urls if pd.to_datetime(dt) > last_dt]

    # Download and run ETL
    for dt in new_months:

        # Split the date string
        month, year = list(map(int, dt.split("/")))

        # Download to temp dir initially
        with tempfile.TemporaryDirectory() as tmpdir:

            # Get the driver
            driver = get_driver(tmpdir)

            # The remote URL
            remote_pdf_path = pdf_urls[dt]

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


@click.group()
@click.version_option()
def phl_budget_sentinel():
    """Parse the City's website to scrape and update City of Philadelphia budget data."""
    pass


@phl_budget_sentinel.command(name="wage")
def update_monthly_wage_collections():
    """Check for updates to the monthly wage collection report."""

    # Get the month/year of last PDF
    year, month = _get_latest_raw_pdf(WageCollectionsBySector)

    # Log
    logger.info(
        f"Checking for PDF report for update since month '{month}' and year '{year}'"
    )

    # Extract out PDF urls on the city's website
    url = f"https://www.phila.gov/documents/{year}-wage-tax-by-industry/"
    css_identifier = "wage-taxes"

    # Run the update
    _run_monthly_update(month, year, url, css_identifier, WageCollectionsBySector)


@phl_budget_sentinel.command(name="city")
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
    logger.info(
        f"Checking for PDF report for update since month '{month}' and year '{year}'"
    )

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


@phl_budget_sentinel.command(name="school")
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
    logger.info(
        f"Checking for PDF report for update since month '{month}' and year '{year}'"
    )

    # Extract out PDF urls on the city's website
    url = f"https://www.phila.gov/documents/fy-{fiscal_year}-school-district-monthly-revenue-collections/"
    css_identifier = "revenue-collections"

    # Run the update
    _run_monthly_update(month, year, url, css_identifier, SchoolTaxCollections)


@phl_budget_sentinel.command(name="rtt")
def update_monthly_rtt_collections():
    """Check for updates to the monthly realty transfer collection report."""

    # Get the month/year of next PDF to look for
    year, month = _get_latest_raw_pdf(RTTCollectionsBySector)

    # Log
    logger.info(
        f"Checking for PDF report for update since month '{month}' and year '{year}'"
    )

    # Extract out PDF urls on the city's website
    url = f"https://www.phila.gov/documents/{year}-realty-transfer-tax-collection/"
    css_identifier = "realty-transfer-tax"

    # Run the update
    _run_monthly_update(month, year, url, css_identifier, RTTCollectionsBySector)
