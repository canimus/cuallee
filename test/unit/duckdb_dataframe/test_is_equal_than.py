import duckdb
import numpy as np
import pandas as pd
import pytest

from cuallee import Check


def test_positive(check: Check, db: duckdb.DuckDBPyConnection):
    check.is_equal_than("id", 1)
    df = pd.DataFrame({"id": np.repeat(1, 10)})
    check.table_name = "df"
    db.register("df", df)
    assert check.validate(db).status.str.match("PASS").all()


def test_negative(check: Check, db: duckdb.DuckDBPyConnection):
    check.is_equal_than("id", 5)
    df = pd.DataFrame({"id": np.arange(10)})
    check.table_name = "df"
    db.register("df", df)
    assert check.validate(db).status.str.match("FAIL").all()


@pytest.mark.parametrize("extra_value", [1, 1.0], ids=("int", "float"))
def test_values(check: Check, db: duckdb.DuckDBPyConnection, extra_value):
    check.is_equal_than("id", extra_value)
    df = pd.DataFrame({"id": [1, 1, 1]})
    check.table_name = "df"
    db.register("df", df)
    assert check.validate(db).status.str.match("PASS").all()


def test_coverage(check: Check, db: duckdb.DuckDBPyConnection):
    check.is_equal_than("id", 1, 0.75)
    df = pd.DataFrame({"id": [1, 1, 1, 0]})
    check.table_name = "df"
    db.register("df", df)
    result = check.validate(db)
    assert result.status.str.match("PASS").all()
    assert result.pass_rate.max() == 0.75
