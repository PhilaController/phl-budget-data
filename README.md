# phl-budget-data

Aggregating and cleaning City of Philadelphia budget-related data

# Installation

You can use the `pip` command:

```
pip install phl_budget_data
```
# Examples

The subsections below list examples for loading various kinds of budget-related data sets for the City of Philadelphia.

## Revenue Reports

Data is available from the City of Philadelphia's Revenue reports, as published to the [City's website](https://www.phila.gov/departments/department-of-revenue/reports/).

### City Collections

Monthly PDF reports are available on the City of Philadelphia's website according to fiscal year (example: [FY 2021](https://www.phila.gov/documents/fy-2021-city-monthly-revenue-collections/)).

**Note:** Cleaned CSV files are available in the following folder: [src/phl_budget_data/data/processed/collections/monthly/city/](src/phl_budget_data/data/processed/collections/monthly/city/)

Load the data using Python:

```python
from phl_budget_data.clean import load_city_collections

data = load_city_collections()
data.head()
```

Output:
```python
                        name  fiscal_year        total month_name  month  fiscal_month  year       date kind
0                      sales         2021   14228731.0        jan      1             7  2021 2021-01-01  Tax
1  wage_earnings_net_profits         2021  182689530.0        jan      1             7  2021 2021-01-01  Tax
2                       soda         2021    5149478.0        jan      1             7  2021 2021-01-01  Tax
3                outdoor_ads         2021     179166.0        jan      1             7  2021 2021-01-01  Tax
4       real_estate_transfer         2021   27222198.0        jan      1             7  2021 2021-01-01  Tax
```
### School District Collections

Monthly PDF reports are available on the City of Philadelphia's website according to fiscal year (example: [FY 2021](https://www.phila.gov/documents/fy-2021-school-district-monthly-revenue-collections/)).

**Note:** Cleaned CSV files are available in the following folder: [src/phl_budget_data/data/processed/collections/monthly/school/](src/phl_budget_data/data/processed/collections/monthly/school/)

Load the data using Python:

```python
from phl_budget_data.clean import load_school_collections

data = load_school_collections()
data.head()
```

Output:

```python
                name  fiscal_year     total month_name  month  fiscal_month  year       date
0        real_estate         2021  50817991        jan      1             7  2021 2021-01-01
1      school_income         2021    436599        jan      1             7  2021 2021-01-01
2  use_and_occupancy         2021  19395530        jan      1             7  2021 2021-01-01
3             liquor         2021   1874302        jan      1             7  2021 2021-01-01
4       other_nontax         2021      2000        jan      1             7  2021 2021-01-01
```

### Monthly Wage Tax Collections by Industry

Monthly PDF reports are available on the City of Philadelphia's website according to calendar year (example: [2020](https://www.phila.gov/documents/2020-wage-tax-by-industry/)).

**Note:** Cleaned CSV files are available in the following folder: [src/phl_budget_data/data/processed/collections/by-sector/wage/](src/phl_budget_data/data/processed/collections/by-sector/wage/)

Load the data using Python:

```python
from phl_budget_data.clean import load_wage_collections_by_sector

data = load_wage_collections_by_sector()
data.head()
```

Output:

```python
                                              sector               parent_sector      total month_name  month  fiscal_month  year  fiscal_year       date
0                              Unclassified Accounts                         NaN   494978.0        jan      1             7  2021         2021 2021-01-01
1                                    Wholesale Trade                         NaN  4497890.0        jan      1             7  2021         2021 2021-01-01
2                 Nursing & Personal Care Facilities  Health and Social Services  3634459.0        jan      1             7  2021         2021 2021-01-01
3  Outpatient Care Centers and Other Health Services  Health and Social Services  6267932.0        jan      1             7  2021         2021 2021-01-01
4  Doctors, Dentists, and Other Health Practitioners  Health and Social Services  5392573.0        jan      1             7  2021         2021 2021-01-01
```


## Quarterly City Manager's Report

PDF reports are available on the City of Philadelphia's website [here](https://www.phila.gov/finance/reports-Quarterly.html).

### Cash Report

Load the data using Python:

```python
from phl_budget_data.clean import load_qcmr_cash_reports

revenue = load_qcmr_cash_reports(kind="revenue")
revenue.head()
```

Output:

```python
                      category  fiscal_month  amount  fiscal_year  quarter  month
0              Real Estate Tax             1     9.1         2021        4      7
1  Wage, Earnings, Net Profits             1   134.1         2021        4      7
2          Realty Transfer Tax             1    36.4         2021        4      7
3                    Sales Tax             1    24.4         2021        4      7
4                         BIRT             1   266.4         2021        4      7
```

Data can be load by specifying `kind` as "revenue", "spending", "fund-balances", or "net-cash-flow".
