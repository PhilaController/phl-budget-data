# phl-budget-data

Aggregating and cleaning City of Philadelphia budget-related data

# Installation

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
from phl_budget_data.clean import load_monthly_tax_collections

data = load_monthly_tax_collections("city")
data.head()
```

Output:
```python
                             name  fiscal_year        total month_name  month  fiscal_month  year       date
0  wage_earnings_net_profits_city         2021  112703449.0        dec     12             6  2020 2020-12-01
1       wage_earnings_net_profits         2021  149179593.0        dec     12             6  2020 2020-12-01
2                       wage_city         2021  111383438.0        dec     12             6  2020 2020-12-01
3                       wage_pica         2021   35437417.0        dec     12             6  2020 2020-12-01
4                            wage         2021  146820855.0        dec     12             6  2020 2020-12-01
```
### School District Collections

Monthly PDF reports are available on the City of Philadelphia's website according to fiscal year (example: [FY 2021](https://www.phila.gov/documents/fy-2021-school-district-monthly-revenue-collections/)).

Load the data:

```python
from phl_budget_data.clean import load_monthly_tax_collections

data = load_monthly_tax_collections("school")
data.head()
```

Output:

```python
                name  fiscal_year     total month_name  month  fiscal_month  year       date
0        real_estate         2021  30509964        dec     12             6  2020 2020-12-01
1      school_income         2021    163926        dec     12             6  2020 2020-12-01
2  use_and_occupancy         2021  15288162        dec     12             6  2020 2020-12-01
3             liquor         2021   2207352        dec     12             6  2020 2020-12-01
4       other_nontax         2021     45772        dec     12             6  2020 2020-12-01
```

### Monthly Wage Tax Collections by Industry

Monthly PDF reports are available on the City of Philadelphia's website according to calendar year (example: [2020](https://www.phila.gov/documents/2020-wage-tax-by-industry/)).


Load the data:

```python
from phl_budget_data.clean import load_wage_collections_by_industry

data = load_wage_collections_by_industry()
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

