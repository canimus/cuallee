import duckdb
import numpy as np
import pandas as pd
import pytest

from cuallee import Check


def test_positive(check: Check, db: duckdb.DuckDBPyConnection):
    check.has_percentile("id", 6.75, 0.75)
    df = pd.DataFrame({"id": np.arange(10)})
    check.table_name = "df"
    db.register("df", df)
    assert check.validate(db).status.str.match("PASS").all()


def test_negative(check: Check, db: duckdb.DuckDBPyConnection):
    check.has_percentile("id", 4.75, 0.75)
    df = pd.DataFrame({"id": np.arange(10)})
    check.table_name = "df"
    db.register("df", df)
    assert check.validate(db).status.str.match("FAIL").all()


@pytest.mark.parametrize(
    "elements", [[1, 2, 3, 4, 5], [0.1, 0.2, 0.3, 0.4, 0.5]], ids=("int", "float")
)
def test_values(check: Check, db: duckdb.DuckDBPyConnection, elements):
    check.has_percentile("id", elements[2], 0.5)
    df = pd.DataFrame({"id": elements})
    check.table_name = "df"
    db.register("df", df)
    assert check.validate(db).status.str.match("PASS").all()


def test_coverage(check: Check):
    with pytest.raises(TypeError):
        check.has_percentile("id", 6.75, 0.8, 10000, 0.8)
