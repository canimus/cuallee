import pandas as pd
from cuallee import Check
import pytest
import duckdb


def test_positive(check: Check, db: duckdb.DuckDBPyConnection):
    check.is_contained_in("id", [0, 1, 2, 3, 4, 5])
    df = pd.DataFrame({"id": range(5)})
    check.table_name = "df"
    assert check.validate(db).status.str.match("PASS").all()


def test_negative(check: Check, db: duckdb.DuckDBPyConnection):
    check.is_contained_in("id", [0, 1, 2, 3])
    df = pd.DataFrame({"id": range(10)})
    check.table_name = "df"
    assert check.validate(db).status.str.match("FAIL").all()


def test_coverage(check: Check, db: duckdb.DuckDBPyConnection):
    check.is_contained_in(
        "id",
        [
            0,
            1,
            2,
            3,
            4,
        ],
        0.50,
    )
    df = pd.DataFrame({"id": range(10)})
    check.table_name = "df"
    result = check.validate(db)
    assert result.status.str.match("PASS").all()
    assert result.pass_rate.max() == 0.5
