import pandas as pd
import numpy as np
from cuallee import Check
import pytest
import duckdb

def test_positive(check: Check, db: duckdb.DuckDBPyConnection):
    check.has_mean("id", 4.5)
    df = pd.DataFrame({"id": np.arange(10)})
    check.table_name = "df"
    assert check.validate(db).status.str.match("PASS").all()


def test_negative(check: Check, db: duckdb.DuckDBPyConnection):
    check.has_mean("id", 5)
    df = pd.DataFrame({"id": np.arange(10)})
    check.table_name = "df"
    assert check.validate(db).status.str.match("FAIL").all()


@pytest.mark.parametrize("extra_value", [4, 4.0], ids=("int", "float"))
def test_values(check: Check, db: duckdb.DuckDBPyConnection, extra_value):
    check.has_mean("id", extra_value)
    df = pd.DataFrame({"id": [0, 1, 2, 3, 14] + [extra_value]})
    check.table_name = "df"
    assert check.validate(db).status.str.match("PASS").all()


def test_coverage(check: Check):
    with pytest.raises(TypeError):
        check.has_mean("id", 5, 0.1)
