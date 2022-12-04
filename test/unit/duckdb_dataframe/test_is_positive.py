import pandas as pd
import numpy as np
from cuallee import Check
import pytest
import duckdb

def test_positive(check: Check, db: duckdb.DuckDBPyConnection):
    check.is_positive("id")
    df = pd.DataFrame({"id": range(1, 10)})
    check.table_name = "df"
    assert check.validate(db).status.str.match("PASS").all()


def test_negative(check: Check, db: duckdb.DuckDBPyConnection):
    check.is_positive("id")
    df = pd.DataFrame({"id": [1, 2, 3, 4, -5]})
    check.table_name = "df"
    assert check.validate(db).status.str.match("FAIL").all()


@pytest.mark.parametrize(
    "values", [[1, 2], [0.1, 0.2], [1e2, 1e3]], ids=("int", "float", "scientific")
)
def test_values(check: Check, db: duckdb.DuckDBPyConnection, values):
    check.is_positive("id")
    df = pd.DataFrame({"id": values})
    check.table_name = "df"
    assert check.validate(db).status.str.match("PASS").all()


def test_coverage(check: Check, db: duckdb.DuckDBPyConnection):
    check.is_positive("id", 0.5)
    df = pd.DataFrame({"id": [1, 2, -1, -2]})
    check.table_name = "df"
    result = check.validate(db)
    assert result.status.str.match("PASS").all()
    assert result.pass_rate.max() == 0.5
