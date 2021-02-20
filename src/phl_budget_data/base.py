"""Abstract base class for performing ETL on PDF reports."""


from abc import ABC, abstractmethod

import pandas as pd


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
