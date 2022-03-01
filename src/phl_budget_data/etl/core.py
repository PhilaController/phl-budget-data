"""Abstract base class for performing ETL on PDF reports."""


import importlib
import inspect
from abc import ABC, abstractmethod
from collections import defaultdict
from pathlib import Path
from typing import List

import pandas as pd
from loguru import logger
from pydantic import BaseModel
from pydantic.main import ModelMetaclass

from .utils.aws import parse_pdf_with_textract


def get_etl_sources():
    """Get all of the ETL sources available."""

    # Current folder and package name
    current_folder = Path(__file__).parent.resolve()
    package_name = str(current_folder).rsplit("src/")[-1].replace("/", ".")

    # Walk this folder
    for f in current_folder.glob("**/*.py"):
        if f.stem.startswith("_"):
            continue

        # Get the relative path
        relative_path = f.relative_to(current_folder)
        module = ".".join(list(relative_path.parts[:-1]) + [f.stem])

        # Import
        importlib.import_module("." + module, package_name)

    # in alphabetical order
    out = defaultdict(list)
    for cls in REGISTRY:

        mod = cls.__module__.replace(package_name + ".", "")
        key = mod.split(".")[0]
        out[key].append(cls)

    return out


def validate_data_schema(data_schema: ModelMetaclass):
    """
    This decorator will validate a pandas.DataFrame against the given data_schema.

    Source
    ------
    https://www.inwt-statistics.com/read-blog/pandas-dataframe-validation-with-pydantic-part-2.html
    """

    def Inner(func):
        def wrapper(*args, **kwargs):
            res = func(*args, **kwargs)
            if isinstance(res, pd.DataFrame):
                # check result of the function execution against the data_schema
                df_dict = res.to_dict(orient="records")

                # Wrap the data_schema into a helper class for validation
                class ValidationWrap(BaseModel):
                    df_dict: List[data_schema]

                # Do the validation
                _ = ValidationWrap(df_dict=df_dict)
            else:
                raise TypeError(
                    "Your Function is not returning an object of type pandas.DataFrame."
                )

            # return the function result
            return res

        return wrapper

    return Inner


REGISTRY = []


class ETLPipeline(ABC):
    """
    An abstract base class to handle the extract-transform-load
    pipeline for parsing a PDF report.
    """

    @classmethod
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if not inspect.isabstract(cls) and "Base" not in cls.__name__:
            REGISTRY.append(cls)

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """Extract the contents of the PDF page."""
        pass

    @abstractmethod
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform the extracted data."""
        pass

    @abstractmethod
    def load(self, data: pd.DataFrame) -> None:
        """Load the transformed data into its data storage."""
        pass

    def _load_csv_data(self, data: pd.DataFrame, path: Path) -> None:
        """Internal function to load CSV data to a specified path."""

        if not path.parent.exists():
            path.parent.mkdir(parents=True)

        logger.info(f"Saving file to {str(path)}")
        data.to_csv(path, index=False)

    @classmethod
    @abstractmethod
    def get_data_directory(cls, kind: str) -> str:
        """Internal function to get the file path.

        Parameters
        ----------
        kind : {'raw', 'processed'}
            type of data to load
        """
        pass

    @classmethod
    def get_pdf_files(cls):
        """Yield raw PDF file paths."""

        yield from sorted(cls.get_data_directory("raw").glob("**/*.pdf"))

    def extract_transform(self) -> pd.DataFrame:
        """Convenience function to extract and then transform."""

        return self.transform(self.extract())

    def validate(self, data) -> bool:
        """
        Validate function to ensure data is correct before
        performing the "load" step.

        By default, returns True.
        """
        return True

    def extract_transform_load(self, validate: bool = True) -> pd.DataFrame:
        """
        Convenience function to extract, transform, and load.

        Optionally, validate the data before loading it.

        Parameters
        ----------
        validate :
            whether to run data validation prior to loading
        """
        # Extract & transform
        data = self.extract_transform()

        # Validate?
        if validate:
            assert self.validate(data) == True

        # Load the data
        self.load(data)

        return data


class ETLPipelineAWS(ETLPipeline):
    """
    Abstract base class for an ETL pipeline that uses
    AWS Textract to parse PDFs.
    """

    def _get_textract_output(self, pg_num, concat_axis=0, remove_headers=False):
        """Use AWS Textract to extract the contents of the PDF."""

        # Get the file name
        interim_dir = self.get_data_directory("interim")
        get_filename = lambda i: interim_dir / f"{self.path.stem}-pg-{i}.csv"

        # The requested file name
        filename = get_filename(pg_num)

        # We need to parse
        if not filename.exists():

            # Initialize the output folder if we need to
            if not interim_dir.is_dir():
                interim_dir.mkdir(parents=True)

            # Extract with textract
            parsing_results = parse_pdf_with_textract(
                self.path,
                bucket_name="phl-budget-data",
                concat_axis=concat_axis,
                remove_headers=remove_headers,
            )

            # Save each page result
            for i, df in parsing_results:
                df.to_csv(get_filename(i), index=False)

        # Return the result
        return pd.read_csv(filename)
