import duckdb
import pandas as pd
import pytest

from cuallee import Check


def test_negative(check: Check, db: duckdb.DuckDBPyConnection):
    check.not_contained_in("id", [0, 1, 2, 3, 4, 5])
    df = pd.DataFrame({"id": range(5)})
    check.table_name = "df"
    db.register("df", df)
    assert check.validate(db).status.str.match("FAIL").all()


def test_positive(check: Check, db: duckdb.DuckDBPyConnection):
    check.not_contained_in("id", [0, 1, 2, 3])
    df = pd.DataFrame({"id": range(10, 20)})
    check.table_name = "df"
    db.register("df", df)
    assert check.validate(db).status.str.match("PASS").all()


def test_coverage(check: Check, db: duckdb.DuckDBPyConnection):
    check.not_contained_in(
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
    db.register("df", df)
    result = check.validate(db)
    assert result.status.str.match("PASS").all()
    assert result.pass_rate.max() == 0.5
