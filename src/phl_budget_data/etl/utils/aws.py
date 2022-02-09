import os
import tempfile
from pathlib import Path

import boto3
import pandas as pd
import pdfplumber
from dotenv import find_dotenv, load_dotenv
from loguru import logger


def parse_pdf_with_textract(pdf_path, bucket_name, resolution=600):
    """Parse the specified PDF with AWS Textract."""

    # Load the credentials
    load_dotenv(find_dotenv())

    # Log
    logger.info(f"Processing pdf '{pdf_path}'")

    # Initialize textract
    textract = boto3.client("textract")

    # Initialize s3
    s3 = boto3.client("s3")

    # Initialize the PDF
    with pdfplumber.open(pdf_path) as pdf:

        with tempfile.TemporaryDirectory() as tmpdir:

            for i, pg in enumerate(pdf.pages):

                # Log the page
                logger.info(f"  Processing page {i+1}...")

                # Create the image and save it to temporary directory
                img = pg.to_image(resolution=resolution)
                filename = Path(f"{tmpdir}/tmp.jpeg")
                img.save(filename)

                # Upload s3 data
                s3.upload_file(str(filename), bucket_name, filename.name)

                # Analyze the document
                r = textract.analyze_document(
                    Document={
                        "S3Object": {"Bucket": bucket_name, "Name": filename.name}
                    },
                    FeatureTypes=["TABLES"],
                )

                # Parse the result
                result = parse_aws_response(r)
                if len(result) > 1:
                    result = pd.concat(result, axis=0).fillna("")
                else:
                    result = result[0]

                yield i + 1, result


def map_blocks(blocks, block_type):
    return {block["Id"]: block for block in blocks if block["BlockType"] == block_type}


def get_children_ids(block):
    """Utility to parse relationships of the results."""
    for rels in block.get("Relationships", []):
        if rels["Type"] == "CHILD":
            yield from rels["Ids"]


def parse_aws_response(r):
    """Parse AWS response from Textract."""

    blocks = r["Blocks"]
    tables = map_blocks(blocks, "TABLE")
    cells = map_blocks(blocks, "CELL")
    words = map_blocks(blocks, "WORD")
    selections = map_blocks(blocks, "SELECTION_ELEMENT")

    # Iterate from top to bottom
    sorted_table_ids = map(
        lambda d: d[1],
        sorted(
            [(v["Geometry"]["BoundingBox"]["Top"], k) for (k, v) in tables.items()],
            key=lambda d: d[0],
        ),
    )

    dataframes = []
    for table_id in sorted_table_ids:
        table = tables[table_id]

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

        # We assume that the first row corresponds to the column names
        dataframe = pd.DataFrame(content)
        dataframes.append(dataframe)

    return dataframes
