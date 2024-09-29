import duckdb
import numpy as np
import pandas as pd
import pytest

from cuallee import Check


def test_positive(check: Check, db: duckdb.DuckDBPyConnection):
    check.has_max("id", 9)
    df = pd.DataFrame({"id": np.arange(10)})
    check.table_name = "df"
    db.register("df", df)
    assert check.validate(db).status.str.match("PASS").all()


def test_negative(check: Check, db: duckdb.DuckDBPyConnection):
    check.has_max("id", 5)
    df = pd.DataFrame({"id": np.arange(10)})
    check.table_name = "df"
    db.register("df", df)
    assert check.validate(db).status.str.match("FAIL").all()


@pytest.mark.parametrize("extra_value", [10, 10001.10001], ids=("int", "float"))
def test_values(check: Check, db: duckdb.DuckDBPyConnection, extra_value):
    check.has_max("id", extra_value)
    df = pd.DataFrame({"id": [0, 1, 2, 3, 4] + [extra_value]})
    check.table_name = "df"
    db.register("df", df)
    assert check.validate(db).status.str.match("PASS").all()


def test_coverage(check: Check):
    with pytest.raises(TypeError):
        check.has_max("id", 5, 0.1)
