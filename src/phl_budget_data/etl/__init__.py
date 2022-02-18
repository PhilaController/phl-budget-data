try:
    import selenium
except ImportError:
    raise Exception("ETL extras not installed; use `pip install phl-budget-data[etl]`")
