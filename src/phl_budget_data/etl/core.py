"""Abstract base class for performing ETL on PDF reports."""

import importlib
import inspect
from abc import ABC, abstractmethod
from collections import defaultdict
from pathlib import Path
from typing import Iterator, Literal, Type

import pandas as pd
from loguru import logger
from pydantic import BaseModel
from pydantic.main import ModelMetaclass

from .utils.aws import parse_pdf_with_textract

# def validate_data_schema(data_schema: ModelMetaclass):
#     """
#     This decorator will validate a pandas.DataFrame against the given data_schema.

#     Source
#     ------
#     https://www.inwt-statistics.com/read-blog/pandas-dataframe-validation-with-pydantic-part-2.html
#     """

#     def Inner(func):
#         def wrapper(*args, **kwargs):
#             res = func(*args, **kwargs)
#             if isinstance(res, pd.DataFrame):
#                 # check result of the function execution against the data_schema
#                 df_dict = res.to_dict(orient="records")

#                 # Wrap the data_schema into a helper class for validation
#                 class ValidationWrap(BaseModel):
#                     df_dict: list[data_schema]

#                 # Do the validation
#                 _ = ValidationWrap(df_dict=df_dict)
#             else:
#                 raise TypeError(
#                     "Your Function is not returning an object of type pandas.DataFrame."
#                 )

#             # return the function result
#             return res

#         return wrapper

#     return Inner


class ETLPipeline(ABC):
    """
    An abstract base class to handle the extract-transform-load
    pipeline for parsing a PDF report.

    Parameters
    ----------
    path :
        The path to the raw PDF file
    """

    path: Path

    @classmethod
    def __init_subclass__(cls, **kwargs):  # type: ignore
        super().__init_subclass__(**kwargs)

        # Add the class to the registry
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

        # Make sure parent exists
        if not path.parent.exists():
            path.parent.mkdir(parents=True)

        # Log it and then save
        logger.info(f"Saving file to {str(path)}")
        data.to_csv(path, index=False)

    @classmethod
    @abstractmethod
    def get_data_directory(cls, kind: Literal["raw", "processed", "interim"]) -> Path:
        """
        Internal function to get the file path.

        Parameters
        ----------
        kind : {'raw', 'processed', 'interim'}
            type of data to load
        """
        pass

    @classmethod
    def get_pdf_files(cls) -> Iterator[Path]:
        """Yield raw PDF file paths."""

        yield from sorted(cls.get_data_directory("raw").glob("**/*.pdf"))

    def extract_transform(self) -> pd.DataFrame:
        """Convenience function to extract and then transform."""

        return self.transform(self.extract())

    def validate(self, data: pd.DataFrame) -> bool:
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


class ETLPipelineAWS(ETLPipeline):  # type: ignore
    """
    Abstract base class for an ETL pipeline that uses
    AWS Textract to parse PDFs.

    This will use Textract to parse tables from a PDF and save
    the results locally.
    """

    def _get_textract_output(
        self, pg_num: int, concat_axis: int = 0, remove_headers: bool = False
    ) -> pd.DataFrame:
        """
        Use AWS Textract to extract the contents of the PDF.

        Parameters
        ----------
        pg_num :
            Which PDF page to parse
        concat_axis :
            If there are multiple tables, combine them along the row or column axis
        remove_headers :
            Whether to trim headers when parsing
        """
        # Get the file name
        interim_dir = self.get_data_directory("interim")
        filename = interim_dir / f"{self.path.stem}-pg-{pg_num}.csv"

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
                path = interim_dir / f"{self.path.stem}-pg-{i}.csv"
                df.to_csv(path, index=False)

        # Return the result
        return pd.read_csv(filename)


def get_etl_sources() -> defaultdict[str, list[Type[ETLPipeline]]]:
    """
    Get all of the ETL sources available.

    The classes are grouped according to their module:
    "qcmr", "collections", "spending".
    """
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

    # In alphabetical order
    out = defaultdict(list)
    for cls in REGISTRY:

        mod = cls.__module__.replace(package_name + ".", "")
        key = mod.split(".")[0]
        out[key].append(cls)

    return out


# Registry for tracking subclasses of ETL pipelines
REGISTRY: list[Type[ETLPipeline]] = []
