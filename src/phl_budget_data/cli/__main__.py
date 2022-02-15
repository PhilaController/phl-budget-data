from dataclasses import fields

import click
import rich_click
from loguru import logger

from ..etl.core import get_etl_sources
from .etl import run_etl

rich_click.core.COMMAND_GROUPS = {"phl-budget-data etl": []}


class RichClickGroup(click.Group):
    def format_help(self, ctx, formatter):
        rich_click.rich_format_help(self, ctx, formatter)


class RichClickCommand(click.Command):
    def format_help(self, ctx, formatter):
        rich_click.rich_format_help(self, ctx, formatter)


@click.group(cls=RichClickGroup)
@click.version_option()
def main():
    """Main command-line interface for working with City of Philadelphia budget data."""
    pass


@main.group(cls=RichClickGroup)
def etl():
    """Run the ETL pipeline for the specified data source."""
    pass


def get_etl_function(source):
    """Create and return an the ETL function for the given source."""

    options = {
        "fiscal_year": "Fiscal year",
        "quarter": "Fiscal quarter",
        "kind": "Either 'adopted' or 'proposed'",
        "year": "Calendar year",
        "month": "Calendar month",
    }
    types = {"kind": click.Choice(["adopted", "proposed"])}

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
        )
        etl_source.params.insert(0, opt)

    return etl_source


def generate_etl_commands():
    """Generate the ETL commands."""

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
            etl_function = get_etl_function(source)

            etl.add_command(etl_function)
            commands.append(source.__name__)

        # Add the help group
        out.append(
            {
                "name": groups[group],
                "commands": sorted(commands),
            }
        )
    return out


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


# Generate the ETL commands and format the CLI help screen
rich_click.core.COMMAND_GROUPS["phl-budget-data etl"] = generate_etl_commands()
