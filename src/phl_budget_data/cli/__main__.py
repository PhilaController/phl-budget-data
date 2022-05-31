import importlib
import itertools
from collections import defaultdict
from pathlib import Path

import click
import numpy as np
import pandas as pd
import rich_click
from loguru import logger
from sqlite_utils import Database

from .. import DATA_DIR
from ..utils import determine_file_name
from .etl import generate_etl_commands, generate_update_commands
from .utils import RichClickCommand, RichClickGroup

rich_click.core.COMMAND_GROUPS = {"phl-budget-data etl": []}


@click.group(cls=RichClickGroup)
@click.version_option()
def main() -> None:
    """Main command-line interface for working with City of Philadelphia budget data."""
    pass


@main.command(cls=RichClickCommand)
@click.option("--output", type=click.Path(exists=False), help="Output folder.")
@click.option("--save-sql", is_flag=True, help="Whether to save SQL databases.")
def save(output: click.Path = None, save_sql: bool = False) -> None:
    """Save the processed data products."""

    if output is None:
        output = DATA_DIR / "processed"
    else:
        output = Path(output)

    for tag in ["spending", "qcmr"]:

        # Output folder
        output_folder = output / tag
        if not output_folder.exists():
            output_folder.mkdir(parents=True)

        # Get the module
        mod = importlib.import_module(f"..etl.{tag}.processed", __package__)

        # Loop over each data loader
        for name in dir(mod):
            if name.startswith("load"):

                # The function
                f = getattr(mod, name)

                # Required params
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

                else:

                    filename = determine_file_name(f).name
                    output_file = output_folder / filename
                    logger.info(f"Saving {output_file}")
                    f().to_csv(output_file, index=False)

    # Save databases too
    if save_sql:
        logger.info("Saving SQL databases")

        # Determine datasets
        datasets = defaultdict(list)
        for f in list((DATA_DIR / "processed").glob("**/*.csv")):
            key = f.parts[-2]
            datasets[key].append(f)

        # Loop over each database
        for key in datasets:

            # Create the database
            filename = DATA_DIR / "sql" / (key + ".db")
            db = Database(filename)

            # Add each dataset
            for f in datasets[key]:
                data = pd.read_csv(f).replace(np.nan, None).to_dict(orient="records")
                db[f.stem].insert_all(data)

        logger.info("...done")


@main.group(cls=RichClickGroup)
def etl():
    """Run the ETL pipeline for the specified data source (development installation only)."""
    pass


@main.group(cls=RichClickGroup)
def update():
    """Parse the City's website to scrape and update City of
    Philadelphia budget data (development installation only)."""
    pass


# Generate the ETL commands and format the CLI help screen
rich_click.core.COMMAND_GROUPS["phl-budget-data etl"] = generate_etl_commands(etl)

# Generate update commands
generate_update_commands(update)
