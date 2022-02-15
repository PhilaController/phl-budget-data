from billy_penn.departments import load_city_departments


def get_department_major_codes():
    """Return the department major codes and names."""

    return (
        load_city_departments()[["dept_name", "dept_code"]]
        .rename(columns={"dept_code": "dept_major_code"})
        .assign(dept_major_code=lambda df: df.dept_major_code.astype(int))
    )
