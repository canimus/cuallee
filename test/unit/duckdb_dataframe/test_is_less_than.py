import pandas as pd
import numpy as np
from cuallee import Check
import pytest
import duckdb


def test_positive(check: Check, db: duckdb.DuckDBPyConnection):
    check.is_less_than("id", 10)
    df = pd.DataFrame({"id": np.arange(10)})
    check.table_name = "df"
    assert check.validate(db).status.str.match("PASS").all()


def test_negative(check: Check, db: duckdb.DuckDBPyConnection):
    check.is_less_than("id", -1)
    df = pd.DataFrame({"id": np.arange(10)})
    check.table_name = "df"
    assert check.validate(db).status.str.match("FAIL").all()


@pytest.mark.parametrize("extra_value", [10, 100010.10001], ids=("int", "float"))
def test_values(check: Check, db: duckdb.DuckDBPyConnection, extra_value):
    check.is_less_than("id", extra_value)
    df = pd.DataFrame({"id": [1, 2, 3, 4]})
    check.table_name = "df"
    assert check.validate(db).status.str.match("PASS").all()


def test_coverage(check: Check, db: duckdb.DuckDBPyConnection):
    check.is_less_than("id", 6, 0.6)
    df = pd.DataFrame({"id": range(10)})
    check.table_name = "df"
    result = check.validate(db)
    assert result.status.str.match("PASS").all()
    assert result.pass_rate.max() == 0.6
