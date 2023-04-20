"""The main command-line interface for phl-budget-data."""

import importlib
import itertools
from pathlib import Path
from typing import Optional

import click
import rich_click
from loguru import logger

from .. import DATA_DIR
from .etl import generate_commands as generate_etl_commands
from .update import generate_commands as generate_update_commands
from .utils import RichClickCommand, RichClickGroup, determine_file_name

# Set up command groups for the "etl" sub-command
rich_click.core.COMMAND_GROUPS = {"phl-budget-data etl": []}


@click.group(cls=RichClickGroup)
@click.version_option()
def main() -> None:
    """Main command-line interface for working with City of Philadelphia budget data."""
    pass


@main.command(cls=RichClickCommand)
@click.option("--output", type=str, help="The output folder.")
@click.option("--save-sql", is_flag=True, help="Whether to save SQL databases.")
def save(output: Optional[str] = None, save_sql: bool = False) -> None:
    """Save the processed data products."""

    # Determine the output path
    if output is None:
        output_path = DATA_DIR / "processed"
    else:
        output_path = Path(output)

    # Loop over each tag
    for tag in ["spending", "qcmr", "collections"]:

        # Handle output folder
        output_folder = output_path / tag
        if not output_folder.exists():
            output_folder.mkdir(parents=True)

        # Get the module
        mod = importlib.import_module(f"..etl.{tag}.processed", __package__)

        # Loop over each data loader
        for name in dir(mod):
            if name.startswith("load"):

                # The function
                f = getattr(mod, name)

                # Function has required params
                if hasattr(f, "model"):

                    # Get the params
                    schema = f.model.schema()
                    params = {
                        k: schema["properties"][k]["enum"] for k in schema["required"]
                    }

                    # Do all iterations of params
                    for param_values in list(itertools.product(*params.values())):

                        kwargs = dict(zip(schema["required"], param_values))
                        data = f(**kwargs)

                        # The filename
                        filename = determine_file_name(f, **kwargs).name
                        output_file = output_folder / filename
                        logger.info(f"Saving {output_file}")
                        data.to_csv(output_file, index=False)
                # Function does not have required params
                else:
                    filename = determine_file_name(f).name
                    output_file = output_folder / filename
                    logger.info(f"Saving {output_file}")
                    f().to_csv(output_file, index=False)


@main.group(cls=RichClickGroup)
def etl():
    """Run the ETL pipeline for the specified data source."""
    pass


@main.group(cls=RichClickGroup)
def update():
    """
    Parse the City's website to scrape and update City of
    Philadelphia budget data.
    """
    pass


# Generate the ETL commands and format the CLI help screen
rich_click.core.COMMAND_GROUPS["phl-budget-data etl"] = generate_etl_commands(etl)

# Generate update commands
generate_update_commands(update)
