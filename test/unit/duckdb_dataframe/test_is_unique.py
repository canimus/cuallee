from cuallee import Check
import pandas as pd
import pytest
import duckdb

def test_positive(check: Check, db: duckdb.DuckDBPyConnection):
    check.is_unique("id")
    df = pd.DataFrame({"id": [10, 20, 30]})
    check.table_name = "df"
    assert check.validate(db).status.str.match("PASS").all()


def test_negative(check: Check, db: duckdb.DuckDBPyConnection):
    check.is_unique("id")
    df = pd.DataFrame({"id": [10, 20, 30, 10]})
    check.table_name = "df"
    assert check.validate(db).status.str.match("FAIL").all()


@pytest.mark.parametrize(
    "values",
    [[0, 1, 2], [0.1, 0.2, 0.0003], [1e2, 1e3, 1e4]],
    ids=("int", "float", "scientific"),
)
def test_parameters(check: Check, db: duckdb.DuckDBPyConnection, values):
    check.is_unique("id")
    df = pd.DataFrame({"id": values})
    check.table_name = "df"
    result = check.validate(db)
    assert result.status.str.match("PASS").all()


def test_coverage(check: Check, db: duckdb.DuckDBPyConnection):
    check.is_unique("id", pct=3 / 4)
    df = pd.DataFrame({"id": [10, 20, 30, 10]})
    check.table_name = "df"
    result = check.validate(db)
    assert result.status.str.match("PASS").all()
    assert result.pass_rate.max() == 3 / 4
