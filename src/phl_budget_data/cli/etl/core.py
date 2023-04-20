import re
from dataclasses import fields
from typing import Optional, Type, TypedDict

import click
from loguru import logger

from ...etl.core import ETLPipeline, get_etl_sources
from ..utils import RichClickCommand


class CommandGroupDict(TypedDict):
    """Group of click commands."""

    name: str
    commands: list[click.Command]


def generate_commands(etl: click.Group) -> list[CommandGroupDict]:
    """Main function for generating the ETL commands."""

    # Mapping from groups to ETL classes
    etl_sources = get_etl_sources()

    # Set up command for running ETL on all Cash Report data products
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
        for source in etl_sources["qcmr"]:

            name = source.__name__
            if name.startswith("CashReport"):
                logger.info(f"Running ETL pipeline for {name}")
                _run_etl(
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
    for group, sources in etl_sources.items():

        # Track the command names
        commands = []

        # Add each source
        for source in sources:

            # Get the etl function for this source
            etl_function = _get_etl_function(source, etl)

            etl.add_command(etl_function)
            commands.append(source.__name__)

        # Add CashReport to QCMR group
        if group == "qcmr":
            etl.add_command(CashReport, name="CashReport")
            commands.append("CashReport")

        # Add the group
        out.append(
            {
                "name": groups[group],
                "commands": sorted(commands),
            }
        )

    return out


def _run_etl(
    cls: Type[ETLPipeline],
    dry_run: bool = False,
    no_validate: bool = False,
    extract_only: bool = False,
    fiscal_year: Optional[int] = None,
    quarter: Optional[int] = None,
    year: Optional[int] = None,
    month: Optional[int] = None,
    **kwargs,
):
    """Internal function to run ETL on the specified class object."""

    # Loop over the PDF files for the class
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
        params = _extract_parameters(f.stem)
        if params is None:
            raise ValueError(f"Could not extract parameters from {f.stem}")

        # ETL
        if not dry_run:

            report = None
            all_params = {**params, **kwargs}

            # Initialize the object
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


def _extract_parameters(s: str) -> Optional[dict[str, int]]:
    """Extract year/quarter/month from a string."""

    # The patterns to try to match
    patterns = [
        "FY(?P<fiscal_year>[0-9]{2})[_-]Q(?P<quarter>[1234])",  # FYXX-QX
        "FY(?P<fiscal_year>[0-9]{2})",  # FYXX
        "(?P<year>[0-9]{4})[_-](?P<month>[0-9]{2})",  # YYYY-MM,
        "(?P<year>[0-9]{4})[_-]Q(?P<quarter>[1234])",  # YYYY-QX
    ]
    for pattern in patterns:
        match = re.match(pattern, s)
        if match:
            d = dict(match.groupdict())
            if "fiscal_year" in d:
                d["fiscal_year"] = "20" + d["fiscal_year"]
            return {k: int(v) for k, v in d.items()}

    return None


def _get_etl_function(source: Type[ETLPipeline], etl: click.Group) -> click.Command:
    """Create and return an the ETL function for the given source."""

    # Create the options dict
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
        _run_etl(
            source,
            dry_run=dry_run,
            no_validate=no_validate,
            extract_only=extract_only,
            **kwargs,
        )

    # Add the keywords
    for field in fields(source):
        opt = click.Option(
            ["--" + field.name.replace("_", "-")],
            type=types.get(field.name, int),
            help=options[field.name] + ".",
            required=field.name in required,
        )
        etl_source.params.insert(0, opt)

    return etl_source