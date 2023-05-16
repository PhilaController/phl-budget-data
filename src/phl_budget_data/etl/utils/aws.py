"""Module for AWS utilities."""

import tempfile
from pathlib import Path
from typing import Iterator, Literal, Optional

import boto3
import pandas as pd
import pdfplumber
from dotenv import find_dotenv, load_dotenv
from loguru import logger
from pydantic import BaseModel


def remove_nonnumeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove any non-numeric headers from the dataframe.

    This will remove any rows where all cells in the row
    are alpha or empty.
    """
    # Loop over index from the beginning
    for i in range(0, len(df.index)):

        label = df.index[i]
        row = df.loc[label]

        # Test if row is all alpha characters or empty string
        test = row.str.match("([A-Za-z\(\)]|^$|20\d{2}|\d{2})").all()
        if not test:
            break

    # Remove everything before the header row
    return df.loc[label:].reset_index(drop=True)


class BoundingBox(BaseModel):
    """Bounding box for a block geometry."""

    Width: float
    Height: float
    Left: float
    Top: float


class XY(BaseModel):
    """X/Y coordinates for a block geometry."""

    X: float
    Y: float


class BlockGeometry(BaseModel):
    """Geometry for a block."""

    BoundingBox: BoundingBox
    Polygon: list[XY]


class Relationship(BaseModel):
    """How does this block relate to other blocks."""

    Type: Literal["VALUE", "CHILD", "COMPLEX_FEATURES", "MERGED_CELL", "TITLE", "TABLE_TITLE", "TABLE_FOOTER"]
    Ids: list[str]


class TextractBlock(BaseModel):
    """Textract block result."""

    BlockType: Literal[
        "KEY_VALUE_SET",
        "PAGE",
        "LINE",
        "WORD",
        "TABLE",
        "CELL",
        "SELECTION_ELEMENT",
        "MERGED_CELL",
        "TITLE",
        "TABLE_TITLE",
        "TABLE_FOOTER"
    ]
    Geometry: BlockGeometry
    Id: str
    Relationships: Optional[list[Relationship]] = None
    Text: str = ""
    Confidence: Optional[float] = None
    RowIndex: int = -1
    ColumnIndex: int = -1
    SelectionStatus: Literal["SELECTED", "NOT_SELECTED", ""] = ""


class TextractResponse(BaseModel):
    """The response from textract's analyze_document()."""

    DocumentMetadata: dict[str, int]
    Blocks: list[TextractBlock]


def parse_pdf_with_textract(
    pdf_path: Path,
    bucket_name: str,
    resolution: int = 600,
    concat_axis: int = 0,
    remove_headers: bool = False,
) -> Iterator[tuple[int, pd.DataFrame]]:
    """
    Parse the specified PDF with AWS Textract.

    Parameters
    ----------
    pdf_path :
        The path to the PDF to parse
    bucket_name :
        The s3 bucket name to upload
    resolution :
        PNG resolution when saving images of PDF to parse
    concat_axis :
        If there are multiple tables, combine them along the row or column axis
    remove_headers :
        Whether to trim non-numeric headers when parsing

    Yields
    ------
    pg_num, data :
        A tuple of the page number and parsed data frame
    """
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

        # Run the analysis in an temp directory
        with tempfile.TemporaryDirectory() as tmpdir:

            # Loop over each page of the PDF
            for pg_num, pg in enumerate(pdf.pages, start=1):

                # Log the page
                logger.info(f"  Processing page #{pg_num}...")

                # Create the image and save it to temporary directory
                img = pg.to_image(resolution=resolution)
                filename = Path(f"{tmpdir}/tmp.jpeg")
                img.save(filename)

                # Upload s3 data
                s3.upload_file(str(filename), bucket_name, filename.name)

                # Analyze the document
                r = TextractResponse.parse_obj(
                    textract.analyze_document(
                        Document={
                            "S3Object": {"Bucket": bucket_name, "Name": filename.name}
                        },
                        FeatureTypes=["TABLES"],
                    )
                )

                # Parse the result
                dataframes = parse_aws_response(r)
                if remove_headers:
                    dataframes = [remove_nonnumeric_columns(df) for df in dataframes]

                # Combine
                if len(dataframes) > 1:

                    # If we are concat'ing along columns, do it from bottom to top
                    if concat_axis == 1:
                        result = pd.concat(dataframes, axis=1).fillna("")
                        result.columns = [str(i) for i in range(0, len(result.columns))]
                    else:
                        result = pd.concat(dataframes)
                else:
                    result = dataframes[0]

                yield pg_num, result


def map_blocks(
    blocks: list[TextractBlock],
    block_type: Literal[
        "KEY_VALUE_SET",
        "PAGE",
        "LINE",
        "WORD",
        "TABLE",
        "CELL",
        "SELECTION_ELEMENT",
        "MERGED_CELL",
        "TITLE",
        "TABLE_TITLE",
        "TABLE_FOOTER"
    ],
) -> dict[str, TextractBlock]:
    return {block.Id: block for block in blocks if block.BlockType == block_type}


def get_children_ids(block: TextractBlock) -> Iterator[str]:
    """Utility to parse relationships of the results."""

    relationships = block.Relationships
    if relationships is not None:
        for rels in relationships:
            if rels.Type == "CHILD":
                yield from rels.Ids


def parse_aws_response(r: TextractResponse) -> list[pd.DataFrame]:
    """Parse AWS response from Textract, returning a list of the parsed data frames."""

    blocks = r.Blocks
    tables = map_blocks(blocks, "TABLE")
    cells = map_blocks(blocks, "CELL")
    words = map_blocks(blocks, "WORD")
    selections = map_blocks(blocks, "SELECTION_ELEMENT")

    # Iterate from top to bottom
    sorted_table_ids = map(
        lambda tup: tup[1],
        sorted(
            [
                (tableBlock.Geometry.BoundingBox.Top, tableId)
                for (tableId, tableBlock) in tables.items()
            ],
            key=lambda tup: tup[0],
        ),
    )

    # Loop over each table
    dataframes = []
    for table_id in sorted_table_ids:
        table = tables[table_id]

        # Determine all the cells that belong to this table
        table_cells = [cells[cell_id] for cell_id in get_children_ids(table)]

        # Determine the table's number of rows and columns
        n_rows = max([cell.RowIndex for cell in table_cells])
        n_cols = max([cell.ColumnIndex for cell in table_cells])
        content = [["" for _ in range(n_cols)] for _ in range(n_rows)]

        # Fill in each cell
        for cell in table_cells:
            cell_contents = [
                words[child_id].Text
                if child_id in words
                else selections[child_id].SelectionStatus
                for child_id in get_children_ids(cell)
            ]
            i = cell.RowIndex - 1
            j = cell.ColumnIndex - 1
            content[i][j] = " ".join(cell_contents)

        # We assume that the first row corresponds to the column names
        dataframe = pd.DataFrame(content)
        dataframes.append(dataframe)

    return dataframes
