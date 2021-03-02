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


Load the data:

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

Load the data:

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


Load the data:

```python
from phl_budget_data.clean import load_wage_collections_by_sector

data = load_wage_collections_by_sector()
data.head()
```

Output:

```python
                                            industry             parent_industry       total month_name  month  fiscal_month  year  fiscal_year       date
0                                  Other Governments                  Government    177693.0        dec     12             6  2020         2021 2020-12-01
1                                    Social Services  Health and Social Services   4631670.0        dec     12             6  2020         2021 2020-12-01
2  Outpatient Care Centers and Other Health Services  Health and Social Services   5302884.0        dec     12             6  2020         2021 2020-12-01
3  Doctors, Dentists, and Other Health Practitioners  Health and Social Services   3390537.0        dec     12             6  2020         2021 2020-12-01
4                                          Hospitals  Health and Social Services  19327622.0        dec     12             6  2020         2021 2020-12-01
```


## Quarterly City Manager's Report

PDF reports are available on the City of Philadelphia's website [here](https://www.phila.gov/finance/reports-Quarterly.html).

*Coming Soon*

