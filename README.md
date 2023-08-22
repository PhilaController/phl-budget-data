
<p align="center">
<img src="static/PHL%20Budget%20Data%20Logo.png"/>
</p>


## Processed data files

The processed data files live in the `src/phl_budget_data/data/processed` folder.
There are three folders:

- `collections/`: This folder includes:
  - `city-collections.csv`: The city's monthly collections, parsed from [public Revenue Dept. reports](https://www.phila.gov/documents/fy-2023-city-monthly-revenue-collections/); includes tax, non-tax, other govt. collections.
  - `city-tax-collections.csv`: The city's monthly tax collections, parsed from [public Revenue Dept. reports](https://www.phila.gov/documents/fy-2023-city-monthly-revenue-collections/); includes only tax collections.
  - `school-collections.csv`: The school district's monthly collections, parsed from [public Revenue Dept. reports](https://www.phila.gov/documents/fy-2023-school-district-monthly-revenue-collections/)
  - `rtt-collections-by-sector.csv`: A breakdown of Realty Transfer Tax collections by sector, parsed from [public Revenue Dept. reports](https://www.phila.gov/documents/2023-realty-transfer-tax-collection/)
  - `sales-collections-by-sector.csv`: A breakdown of Sales Tax collections by sector, parsed from [public Revenue Dept. reports](https://www.phila.gov/documents/annual-sales-tax-collections-reports/)
  - `wage-collection-by-sector.csv`: A breakdown of Wage Tax collections by sector, parsed from [public Revenue Dept. reports](https://www.phila.gov/documents/2023-wage-tax-by-industry/)
- `qcmr/`: This folder includes data parsed from the Quarterly City Manager's Report (QCMR):
  - `cash-reports-*.csv`: Data parsed from different parts of the Cash Report in the back of the QCMR
  - `department-obligations.csv`: Data parsed from the Departmental Obligations table in the QCMR
  - `fulltime-positions.csv`: Data parsed from the Fulltime Positions Report table in the QCMR
  - `personal-services-summary.csv`: Data parsed from the Personal Services Summary table in the QCMR
- `spending/`: This folder includes data parsed from City Budget-in-Brief documents:
  - `actual-department-spending.csv`: Historical actual spending by department
  - `budgeted-department-spending-adopted.csv`: Budgeted spending by department from the adopted budget
  - `budgeted-department-spending-proposed.csv`: Budgeted spending by department from the proposed budget

## Development set up

First clone the environment:

```bash
git clone https://github.com/PhiladelphiaController/phl-budget-data.git
```

Then, install the Python dependencies with poetry:

```bash
cd phl-budget-data
poetry install
```

And run the help message for the main command:

```bash
poetry run phl-budget-data --help
```

You will need AWS credentials for running the parsing scripts. Create a `.env` file in the root of the project
that is mirrored off of `.env.example` and fill in the values. To get the AWS 
credentials, go to the "Credentials/" folder on the FPD Sharepoint.


## Adding new data

In general, the process for adding new data is:

1. Add the raw PDF files to the appropriate folder in `src/phl_budget_data/data/etl/raw`. Look at past PDF files to make sure you are adding the correct table to the correct folder. You should make sure to add a PDF that only contains the pages with the table information.
2. Run the appropriate ETL command for the data you are parsing; run `poetry run phl-budget-data etl --help` to see the available commands. For example, to parse the cash report data, run `poetry run phl-budget-data etl CashReport`. This will create a new CSV file in the appropriate folder in `src/phl_budget_data/etl/data/processed`.
3. Update the files in the processed data folder `src/phl_budget_data/data/processed` by saving new versions: `poetry run phl-budget-data save`.

### Example: Adding new cash report data

1. Extract out the two-page cash report PDF from the latest QCMR and save it to: `src/phl_budget_data/data/etl/raw/qcmr/cash/`.
2. Run the ETL parsing command. For example, for FY23 Q4 you would run: `poetry run phl-budget-data etl CashReport --fiscal-year 2023 --quarter 4`.
3. Update the main processed data files: `poetry run phl-budget-data save`.

## Automatic updates for monthly collections

There is a GitHub action in this repository that runs daily and checks the City's website for newly uploaded monthly collection
reports. These reports are uploaded to the City's [revenue reports](https://www.phila.gov/departments/department-of-revenue/reports/) with
about a month delay. The script checks for new data and will parse and save it to the repository if it finds a new report.