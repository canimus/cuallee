import pandas as pd
import numpy as np
from cuallee import Check
import pytest
import duckdb


def test_positive(check: Check, db: duckdb.DuckDBPyConnection):
    check.has_min("id", 0)
    df = pd.DataFrame({"id": np.arange(10)})
    check.table_name = "df"
    assert check.validate(db).status.str.match("PASS").all()


def test_negative(check: Check, db: duckdb.DuckDBPyConnection):
    check.has_min("id", 5)
    df = pd.DataFrame({"id": np.arange(10)})
    check.table_name = "df"
    assert check.validate(db).status.str.match("FAIL").all()


@pytest.mark.parametrize("extra_value", [0, 0.10001], ids=("int", "float"))
def test_values(check: Check, db: duckdb.DuckDBPyConnection, extra_value):
    check.has_min("id", extra_value)
    df = pd.DataFrame({"id": [1, 2, 3, 4] + [extra_value]})
    check.table_name = "df"
    assert check.validate(db).status.str.match("PASS").all()


def test_coverage(check: Check):
    with pytest.raises(TypeError):
        check.has_min("id", 5, 0.1)
