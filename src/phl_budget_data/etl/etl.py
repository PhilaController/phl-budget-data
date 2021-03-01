"""Abstract base class for performing ETL on PDF reports."""


from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd
from loguru import logger


class ETLPipeline(ABC):
    """
    An abstract base class to handle the extract-transform-load
    pipeline for parsing a PDF report.
    """

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

    def get_pdf_files(self):
        """Yield raw PDF file paths."""

        yield from sorted(self.get_data_directory("raw").glob("*.pdf"))

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
