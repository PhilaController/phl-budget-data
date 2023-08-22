
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
- 

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


