"""Module implementing the update command for the phl-budget-data CLI."""

import tempfile
from typing import Tuple, Type
from urllib.error import HTTPError

import click
import pandas as pd
from loguru import logger

from ...etl import collections
from ...etl.core import ETLPipeline
from .scrape import cwd, downloaded_pdf, extract_pdf_urls, get_scraping_driver


def generate_commands(update: click.Group) -> None:
    """Generate the subcommands for the "update" command."""

    @update.command(name="wage")
    def update_monthly_wage_collections():
        """Check for updates to the monthly wage collection report."""

        # Get the month/year of last PDF
        cls = collections.WageCollectionsBySector
        year, month = _get_latest_raw_pdf(cls)
        latest_date = pd.to_datetime(f"{month}/{year}")

        # Log
        logger.info(
            f"Checking for PDF report for update since month '{month}' and year '{year}'"
        )

        # Do we need to move to the next calendar year?
        if month == 12:
            year += 1

        # Extract out PDF urls on the city's website
        url = f"https://www.phila.gov/documents/{year}-wage-tax-by-industry/"
        css_identifier = "wage-taxes"

        # Run the update
        _run_monthly_update(month, year, latest_date, url, css_identifier, cls)

    @update.command(name="city")
    def update_monthly_city_collections():
        """Check for updates to the monthly city collection report."""

        # Get the month/year of next PDF to look for
        year, month = _get_latest_raw_pdf(collections.CityTaxCollections)
        latest_date = pd.to_datetime(f"{month}/{year}")

        # Log
        logger.info(
            f"Checking for PDF report for update since month '{month}' and year '{year}'"
        )

        # Get the fiscal year
        if month < 7:
            fiscal_year = year
        else:
            fiscal_year = year + 1

        # Do we need to move to the next fiscal year?
        if month == 6:
            fiscal_year += 1

        # Extract out PDF urls on the city's website
        url = f"https://www.phila.gov/documents/fy-{fiscal_year}-city-monthly-revenue-collections/"
        css_identifier = "revenue-collections"

        # Run the update
        _run_monthly_update(
            month,
            year,
            latest_date,
            url,
            css_identifier,
            collections.CityTaxCollections,
            collections.CityNonTaxCollections,
            collections.CityOtherGovtsCollections,
        )

    @update.command(name="school")
    def update_monthly_school_collections():
        """Check for updates to the monthly school district collection report."""

        # Get the month/year of next PDF to look for
        cls = collections.SchoolTaxCollections
        year, month = _get_latest_raw_pdf(cls)
        latest_date = pd.to_datetime(f"{month}/{year}")

        # Log
        logger.info(
            f"Checking for PDF report for update since month '{month}' and year '{year}'"
        )

        # Get the fiscal year
        if month < 7:
            fiscal_year = year
        else:
            fiscal_year = year + 1

        # Do we need to move to the next fiscal year?
        if month == 6:
            fiscal_year += 1

        # Log
        logger.info(
            f"Checking for PDF report for update since month '{month}' and year '{year}'"
        )

        # Extract out PDF urls on the city's website
        url = f"https://www.phila.gov/documents/fy-{fiscal_year}-school-district-monthly-revenue-collections/"
        css_identifier = "revenue-collections"

        # Run the update
        _run_monthly_update(month, year, latest_date, url, css_identifier, cls)

    # Add the subcommands
    update.add_command(update_monthly_wage_collections, name="wage")
    update.add_command(update_monthly_city_collections, name="city")
    update.add_command(update_monthly_school_collections, name="school")


def _get_latest_raw_pdf(cls: Type[ETLPipeline]) -> Tuple[int, int]:
    """Given an ETL class, return the latest PDF in the data directory."""

    # Get PDF paths for the raw data files
    dirname = cls.get_data_directory("raw")
    pdf_files = dirname.glob("*.pdf")

    # Get the latest
    latest = sorted(pdf_files)[-1]
    year, month = map(int, latest.stem.split("_"))

    return year, month


def _run_monthly_update(
    month: int,
    year: int,
    latest_date: pd.Timestamp,
    url: str,
    css_identifier: str,
    *etls: Type[ETLPipeline],
) -> None:
    """
    Internal function to run update on monthly PDFs.

    Parameters
    ----------
    month : int
        the latest month that we have data for
    year : int
        the latest calendar year we have data for
    url : str
        the url to check
    css_identifier : str
        the element identifer to scrape
    etls : list
        the ETL classes to run
    """

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
    new_months = [dt for dt in pdf_urls if pd.to_datetime(dt) > latest_date]

    # Download and run ETL
    for dt in new_months:

        # Split the date string
        month, year = list(map(int, dt.split("/")))

        # Download to temp dir initially
        with tempfile.TemporaryDirectory() as tmpdir:

            # Change the path
            with cwd(tmpdir):

                # Get the driver
                driver = get_scraping_driver(tmpdir)

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

    if not len(new_months):
        logger.info(f"...no updates found")
