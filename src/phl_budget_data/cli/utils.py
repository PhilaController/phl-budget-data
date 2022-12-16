"""Utilities module for command-line interface."""

from __future__ import annotations

from typing import List

import click
import rich_click

from .. import DATA_DIR


def determine_file_name(f, **kwargs):
    """Given a function, determine the matching file name."""

    # The parts
    name = f.__name__
    tag = f.__module__.split(".")[-1]

    # The base of the file name
    filename_base = "-".join(name.split("_")[1:])

    # The output folder
    output_folder = DATA_DIR / "historical" / tag

    # Function has required params
    if hasattr(f, "model"):

        # Get the params
        schema = f.model.schema()

        # Do all iterations of params
        param_values: List[str] = [kwargs.get(k, "") for k in schema["required"]]

        # If any are missing raise an error
        if any(value == "" for value in param_values):
            raise ValueError("Missing required params")

        # The filename
        filename = filename_base + "-" + "-".join(param_values) + ".csv"
        output_file = output_folder / filename
    else:
        filename = "-".join(name.split("_")[1:]) + ".csv"
        output_file = output_folder / filename

    return output_file


class RichClickGroup(click.Group):
    def format_help(self, ctx, formatter):
        rich_click.rich_format_help(self, ctx, formatter)


class RichClickCommand(click.Command):
    def format_help(self, ctx, formatter):
        rich_click.rich_format_help(self, ctx, formatter)
