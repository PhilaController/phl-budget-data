# from typing import Literal

# import pandas as pd
# from pydantic import validate_arguments

# from .utils import ETL_VERSION, optional_from_cache

# if ETL_VERSION:
#     from .etl import spending

# __all__ = ["load_budgeted_department_spending", "load_actual_department_spending"]


# @optional_from_cache
# @validate_arguments
# def load_budgeted_department_spending(kind: Literal["adopted", "proposed"]):
#     """
#     Load budgeted spending by department and major class.

#     Source: Annual Budget-in-Brief documents
#     """

#     # Get the dirname
#     dirname = (
#         spending.BudgetedDepartmentSpending.get_data_directory("processed")
#         / kind
#         / "budget"
#     )

#     # Glob the PDF files
#     out = []
#     for f in sorted(dirname.glob("*.csv")):
#         out.append(pd.read_csv(f, dtype={"dept_code": str, "dept_major_code": str}))

#     # Combine
#     out = pd.concat(out, ignore_index=True)

#     return out


# @optional_from_cache
# def load_actual_department_spending():
#     """
#     Load actual spending by department and major class.

#     Source: Annual Budget-in-Brief documents
#     """
#     # Get the dirname
#     dirname = spending.ActualDepartmentSpending.get_data_directory("processed")

#     # Glob the PDF files
#     out = []
#     for f in sorted(dirname.glob("*/actual/*.csv")):
#         out.append(pd.read_csv(f, dtype={"dept_code": str, "dept_major_code": str}))

#     out = pd.concat(out, ignore_index=True).drop_duplicates(
#         subset=["dept_code", "fiscal_year"], keep="first"
#     )

#     return out
