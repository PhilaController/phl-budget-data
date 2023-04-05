"""Scraping utilities for getting data from phila.gov."""

import calendar
import os
import time
from contextlib import contextmanager
from pathlib import Path
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager

MONTH_LOOKUP = [x.lower() for x in calendar.month_abbr[1:]]


@contextmanager
def cwd(path):
    oldpwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(oldpwd)


def parse_website(url: str) -> str:
    """Parse the input website."""

    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    web_byte = urlopen(req).read()
    return web_byte.decode("utf-8")


def extract_pdf_urls(url: str, css_identifier: str) -> dict[str, str]:
    """Extract PDF urls from the input URL."""

    # Parse the website
    soup = BeautifulSoup(parse_website(url), features="html.parser")

    # Get the id of the element
    table_trs = soup.select(f"table tr[id*={css_identifier}]")

    out = {}
    for tr in table_trs:
        # Get the url
        a = tr.select_one("a")
        if a is not None:
            pdf_url = a["href"]
        else:
            raise RuntimeError("Parsing error")

        # Get the element's ID
        id_str = tr.attrs["id"]

        # Extract out month name and year
        # NOTE: THIS COULD EASILY BREAK
        fields = id_str.split("-")
        if "-to-" in id_str:
            month_name = fields[2][:3]
            year = int(fields[3])
        else:
            month_name = fields[0][:3]
            year = int(fields[1])
        month_num = MONTH_LOOKUP.index(month_name) + 1

        # Save it
        out[f"{month_num}/{year}"] = pdf_url

    # Return
    return out


def get_scraping_driver(dirname):
    """Load the driver."""

    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--headless")

    # Setup the download directory for PDFs
    profile = {
        "plugins.plugins_list": [
            {"enabled": False, "name": "Chrome PDF Viewer"}
        ],  # Disable Chrome's PDF Viewer
        "download.default_directory": dirname,
        "download.extensions_to_open": "applications/pdf",
    }
    options.add_experimental_option("prefs", profile)

    # Initialize with options
    driver = webdriver.Chrome(
        executable_path=ChromeDriverManager().install(), options=options
    )

    return driver


@contextmanager
def downloaded_pdf(driver, pdf_url, tmpdir, interval=1, time_limit=7):
    """Context manager to download a PDF to a local directory."""

    # Output path
    download_dir = Path(tmpdir)
    pdf_path = None

    try:
        # Get the PDF
        driver.get(pdf_url)

        # Initialize
        pdf_files = list(download_dir.glob("*.pdf"))
        total_sleep = 0
        while not len(pdf_files) and total_sleep <= time_limit:
            time.sleep(interval)
            total_sleep += interval
            pdf_files = list(download_dir.glob("*.pdf"))

        if len(pdf_files):
            pdf_path = pdf_files[0]
            yield pdf_path
        else:
            raise ValueError("PDF download failed")
    finally:

        # Remove the file after we are done!
        if pdf_path is not None and pdf_path.exists():
            pdf_path.unlink()
