import re

from loguru import logger


def extract_parameters(s):
    """Extract year/quarter/month from a string."""

    # The patterns to try to match
    patterns = [
        "FY(?P<fiscal_year>[0-9]{2})[_-]Q(?P<quarter>[1234])",  # FYXX-QX
        "FY(?P<fiscal_year>[0-9]{2})",  # FYXX
        "(?P<year>[0-9]{4})[_-](?P<month>[0-9]{2})",  # YYYY-MM
    ]
    for pattern in patterns:
        match = re.match(pattern, s)
        if match:
            d = match.groupdict()
            if "fiscal_year" in d:
                d["fiscal_year"] = "20" + d["fiscal_year"]
            return {k: int(v) for k, v in d.items()}

    return None


def run_etl(
    cls,
    dry_run=False,
    no_validate=False,
    extract_only=False,
    fiscal_year=None,
    quarter=None,
    year=None,
    month=None,
    **kwargs,
):
    """Internal function to run ETL on fiscal year data."""

    # Loop over the PDF files
    finished_params = []
    for f in cls.get_pdf_files():

        # Filter by fiscal year
        if fiscal_year is not None:
            pattern = f"FY{str(fiscal_year)[2:]}"
            if pattern not in f.stem:
                continue

        # Filter by quarter
        if quarter is not None:
            pattern = f"Q{quarter}"
            if pattern not in f.stem:
                continue

        # Filter by year
        if year is not None:
            pattern = f"{year}"
            if pattern not in f.stem:
                continue

        # Filter by month
        if month is not None:
            pattern = f"{month:02d}"
            if pattern not in f.stem:
                continue

        # Extract parameters
        params = extract_parameters(f.stem)
        if params is None:
            raise ValueError(f"Could not extract parameters from {f.stem}")

        # ETL
        if not dry_run:

            report = None
            all_params = {**params, **kwargs}

            try:
                report = cls(**all_params)
            except FileNotFoundError:
                pass
            all_params_tup = tuple(all_params.items())

            # Run the ETL pipeline
            if report and all_params_tup not in finished_params:

                # Log it
                finished_params.append(all_params_tup)
                s = ", ".join(f"{k}={v}" for k, v in all_params.items())
                logger.info(f"Processing: {s}")

                if not extract_only:
                    report.extract_transform_load(validate=(not no_validate))
                else:
                    report.extract()
