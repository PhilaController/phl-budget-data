import inspect
from functools import wraps

import pandas as pd

from . import DATA_DIR, ETL_VERSION


def determine_file_name(f, **kwargs):
    """Determine the file name."""

    # The parts
    name = f.__name__
    tag = f.__module__.split(".")[-1]

    # The base of the file name
    filename_base = "-".join(name.split("_")[1:])

    # The output folder
    output_folder = DATA_DIR / "historical" / tag

    # Required params
    if hasattr(f, "model"):

        # Get the params
        schema = f.model.schema()

        # Do all iterations of params
        param_values = [kwargs.get(k) for k in schema["required"]]
        if any(value is None for value in param_values):
            raise ValueError("Missing required params")

        # The filename
        filename = filename_base + "-" + "-".join(param_values) + ".csv"
        output_file = output_folder / filename
    else:
        filename = "-".join(name.split("_")[1:]) + ".csv"
        output_file = output_folder / filename

    if not output_file.exists():
        raise FileNotFoundError(f"File not found: {output_file}")

    return output_file


def optional_from_cache(f):
    """Decorator to check if ETL is installed and load from cache."""

    @wraps(f)
    def wrapper(*args, **kwargs):

        # Get the signature
        sig = inspect.signature(f).bind(*args, **kwargs)

        if not ETL_VERSION:
            filename = determine_file_name(f, **sig.arguments)
            return pd.read_csv(filename)
        else:
            return f(*args, **kwargs)

    return wrapper
