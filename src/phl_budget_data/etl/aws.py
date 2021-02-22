import os
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import ClassVar

import boto3
import pandas as pd
from dotenv import load_dotenv

from .utils.pdf import Word


def map_blocks(blocks, block_type):
    return {block["Id"]: block for block in blocks if block["BlockType"] == block_type}


def textract_to_words(response):
    """Return words from AWS Textract response."""

    # Extract block types
    blocks = response["Blocks"]
    words = map_blocks(blocks, "WORD")

    out = defaultdict(list)
    for k in words:
        w = words[k]
        bbox = w["Geometry"]["BoundingBox"]
        out[w["Page"]].append(
            Word(
                x0=bbox["Left"],
                x1=bbox["Left"] + bbox["Width"],
                top=bbox["Top"],
                bottom=bbox["Top"] + bbox["Height"],
                text=w["Text"],
            )
        )

    for pg_num in sorted(out):
        yield out[pg_num]


def textract_to_table(response):
    """Yield tables from AWS Textract response as pandas DataFrames."""

    # Extract block types
    blocks = response["Blocks"]
    tables = map_blocks(blocks, "TABLE")
    cells = map_blocks(blocks, "CELL")
    words = map_blocks(blocks, "WORD")
    selections = map_blocks(blocks, "SELECTION_ELEMENT")

    def get_children_ids(block):
        for rels in block.get("Relationships", []):
            if rels["Type"] == "CHILD":
                yield from rels["Ids"]

    # Look over each of the tables
    for table in tables.values():

        # Determine all the cells that belong to this table
        table_cells = [cells[cell_id] for cell_id in get_children_ids(table)]

        # Determine the table's number of rows and columns
        n_rows = max(cell["RowIndex"] for cell in table_cells)
        n_cols = max(cell["ColumnIndex"] for cell in table_cells)
        content = [[None for _ in range(n_cols)] for _ in range(n_rows)]

        # Fill in each cell
        for cell in table_cells:
            cell_contents = [
                words[child_id]["Text"]
                if child_id in words
                else selections[child_id]["SelectionStatus"]
                for child_id in get_children_ids(cell)
            ]
            i = cell["RowIndex"] - 1
            j = cell["ColumnIndex"] - 1
            content[i][j] = " ".join(cell_contents)

        yield table["Page"], pd.DataFrame(content)


@dataclass
class AWSTextract:
    """
    Interface for AWS Textract to extract tables from PDFs.

    Notes
    -----
    - Data is uploaded to AWS s3 before passing to Textract.
    - The 'AWS_ACCESS_KEY' and 'AWS_SECRET_KEY' keys should be
    specified as environment files.

    Parameters
    ----------
    path :
        the local path to the PDF to extract tables from
    """

    path: str
    BUCKET: ClassVar[str] = "phl-budget-data"

    def __post_init__(self):
        """Initialize the AWS clients."""

        # Load env variables
        load_dotenv()

        # Make sure we have the keys
        aws_access_key_id = os.environ.get("AWS_ACCESS_KEY")
        if aws_access_key_id is None:
            raise ValueError("Specify AWS_ACCESS_KEY as environment variable")
        aws_secret_access_key = os.environ.get("AWS_SECRET_KEY")
        if aws_secret_access_key is None:
            raise ValueError("Specify AWS_SECRET_KEY as environment variable")

        # Initialize the aws clients
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

        self.textract = boto3.client(
            "textract",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

        # Create the bucket if we need to
        self.bucket = self.s3.create_bucket(Bucket=self.BUCKET)

    def extract(self):
        """Extract data using AWS Textract."""

        # Upload the PDF file to s3
        self.s3.upload_file(str(self.path), self.BUCKET, self.path.name)

        # Start the document analysis
        r = self.textract.start_document_analysis(
            DocumentLocation={
                "S3Object": {"Bucket": self.BUCKET, "Name": self.path.name}
            },
            FeatureTypes=[
                "TABLES",
            ],
        )

        # Wait until job has finished
        jobstatus = None
        while jobstatus != "SUCCEEDED":
            response = self.textract.get_document_analysis(JobId=r["JobId"])
            jobstatus = response["JobStatus"]
            time.sleep(1)

        # Yield pg numbers and table
        yield from textract_to_words(response)
