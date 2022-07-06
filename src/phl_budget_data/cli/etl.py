import re
import tempfile
from dataclasses import fields
from urllib.error import HTTPError

import click
import pandas as pd
from loguru import logger

from ..etl import collections
from ..etl.core import get_etl_sources
from .scrape import downloaded_pdf, extract_pdf_urls, get_driver
from .utils import RichClickCommand


def extract_parameters(s):
    """Extract year/quarter/month from a string."""

    # The patterns to try to match
    patterns = [
        "FY(?P<fiscal_year>[0-9]{2})[_-]Q(?P<quarter>[1234])",  # FYXX-QX
        "FY(?P<fiscal_year>[0-9]{2})",  # FYXX
        "(?P<year>[0-9]{4})[_-](?P<month>[0-9]{2})",  # YYYY-MM
    ]
    for pattern in patterns:
        match = re.match(pattern, s)
        if match:
            d = match.groupdict()
            if "fiscal_year" in d:
                d["fiscal_year"] = "20" + d["fiscal_year"]
            return {k: int(v) for k, v in d.items()}

    return None


def run_etl(
    cls,
    dry_run=False,
    no_validate=False,
    extract_only=False,
    fiscal_year=None,
    quarter=None,
    year=None,
    month=None,
    **kwargs,
):
    """Internal function to run ETL on fiscal year data."""

    # Loop over the PDF files
    finished_params = []
    for f in cls.get_pdf_files():

        # Filter by fiscal year
        if fiscal_year is not None:
            pattern = f"FY{str(fiscal_year)[2:]}"
            if pattern not in f.stem:
                continue

        # Filter by quarter
        if quarter is not None:
            pattern = f"Q{quarter}"
            if pattern not in f.stem:
                continue

        # Filter by year
        if year is not None:
            pattern = f"{year}"
            if pattern not in f.stem:
                continue

        # Filter by month
        if month is not None:
            pattern = f"{month:02d}"
            if pattern not in f.stem:
                continue

        # Extract parameters
        params = extract_parameters(f.stem)
        if params is None:
            raise ValueError(f"Could not extract parameters from {f.stem}")

        # ETL
        if not dry_run:

            report = None
            all_params = {**params, **kwargs}

            try:
                report = cls(**all_params)
            except FileNotFoundError:
                pass
            all_params_tup = tuple(all_params.items())

            # Run the ETL pipeline
            if report and all_params_tup not in finished_params:

                # Log it
                finished_params.append(all_params_tup)
                s = ", ".join(f"{k}={v}" for k, v in all_params.items())
                logger.info(f"Processing: {s}")

                if not extract_only:
                    report.extract_transform_load(validate=(not no_validate))
                else:
                    report.extract()


def get_etl_function(source, etl):
    """Create and return an the ETL function for the given source."""

    options = {
        "fiscal_year": "Fiscal year",
        "quarter": "Fiscal quarter",
        "kind": "Either 'adopted' or 'proposed'",
        "year": "Calendar year",
        "month": "Calendar month",
    }
    types = {"kind": click.Choice(["adopted", "proposed"])}
    required = ["kind"]

    @etl.command(
        cls=RichClickCommand,
        name=source.__name__,
        help=source.__doc__,
    )
    @click.option("--dry-run", is_flag=True, help="Do not save any new files.")
    @click.option("--no-validate", is_flag=True, help="Do not validate the data.")
    @click.option(
        "--extract-only",
        is_flag=True,
        help="Only extract the data (do not transform/load).",
    )
    def etl_source(dry_run, no_validate, extract_only, **kwargs):

        # Run the ETL
        logger.info(f"Running ETL pipeline for {source.__name__}")
        run_etl(
            source,
            dry_run=dry_run,
            no_validate=no_validate,
            extract_only=extract_only,
            **kwargs,
        )

    # Add
    for field in fields(source):
        opt = click.Option(
            ["--" + field.name.replace("_", "-")],
            type=types.get(field.name, int),
            help=options[field.name] + ".",
            required=field.name in required,
        )
        etl_source.params.insert(0, opt)

    return etl_source


def generate_etl_commands(etl):
    """Generate the ETL commands."""

    @etl.command(cls=RichClickCommand, name="CashReport")
    @click.option("--fiscal-year", help="Fiscal year.", type=int)
    @click.option("--quarter", help="Fiscal quarter.", type=int)
    @click.option("--dry-run", is_flag=True, help="Do not save any new files.")
    @click.option("--no-validate", is_flag=True, help="Do not validate the data.")
    @click.option(
        "--extract-only",
        is_flag=True,
        help="Only extract the data (do not transform/load).",
    )
    def CashReport(dry_run, no_validate, extract_only, fiscal_year, quarter):
        "Run ETL on all Cash Report sources from the QCMR."

        # Run the ETL for Cash Report
        for source in get_etl_sources()["qcmr"]:

            name = source.__name__
            if name.startswith("CashReport"):
                logger.info(f"Running ETL pipeline for {name}")
                run_etl(
                    source,
                    dry_run,
                    no_validate,
                    extract_only,
                    fiscal_year=fiscal_year,
                    quarter=quarter,
                )

    # Names of the groups
    groups = {
        "qcmr": "QCMR",
        "collections": "Collections",
        "spending": "Spending",
    }
    out = []

    # Loop over each group
    for group, sources in get_etl_sources().items():

        # Track the command names
        commands = []

        # Add each source
        for source in sources:

            # Get the etl function for this source
            etl_function = get_etl_function(source, etl)

            etl.add_command(etl_function)
            commands.append(source.__name__)

        # add CashReport to QCMR
        if group == "qcmr":
            etl.add_command(CashReport, name="CashReport")
            commands.append("CashReport")

        # Add the help group
        out.append(
            {
                "name": groups[group],
                "commands": sorted(commands),
            }
        )

    return out


def _get_latest_raw_pdf(cls):
    """Given an ETL class, return the latest PDF in the data directory."""

    # Get PDF paths for the raw data files
    dirname = cls.get_data_directory("raw")
    pdf_files = dirname.glob("*.pdf")

    # Get the latest
    latest = sorted(pdf_files)[-1]
    year, month = map(int, latest.stem.split("_"))

    return year, month


def _run_monthly_update(month, year, latest_date, url, css_identifier, *etls):
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

    if not len(new_months):
        logger.info(f"...no updates found")


def generate_update_commands(update):
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
