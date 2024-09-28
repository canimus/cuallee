import pandas as pd
from cuallee import Check
import pytest
from datetime import datetime, timedelta
import duckdb


def test_positive(check: Check, db: duckdb.DuckDBPyConnection):
    check.is_daily("id")
    df = pd.DataFrame(
        {"id": pd.date_range(start="2022-01-01", end="2022-02-01", freq="D")}
    )
    check.table_name = "df"
    db.register("df", df)
    assert check.validate(db).status.str.match("PASS").all()


def test_parameters(check: Check, db: duckdb.DuckDBPyConnection):
    check.is_daily("id", [1, 2, 3, 4, 5])
    df = pd.DataFrame(
        {"id": pd.date_range(start="2022-01-01", end="2022-02-01", freq="D")}
    )
    check.table_name = "df"
    db.register("df", df)
    assert check.validate(db).status.str.match("PASS").all()


def test_violations(check: Check, db: duckdb.DuckDBPyConnection):
    check.is_daily("id")
    df = pd.DataFrame(
        {"id": pd.date_range(start="2022-01-01", end="2022-02-01", freq="D")}
    )
    df = pd.concat([df, pd.DataFrame({"id": [pd.Timestamp("2022-12-01")]})])

    check.table_name = "df"
    db.register("df", df)
    assert check.validate(db).status.str.match("FAIL").all()


def test_negative(check: Check, db: duckdb.DuckDBPyConnection):
    check.is_daily("id")
    df = pd.DataFrame(
        {
            "id": [
                datetime.fromisoformat("2022-01-01") + timedelta(days=i)
                for i in range(1, 10, 2)
            ]
        }
    )
    check.table_name = "df"
    db.register("df", df)
    assert check.validate(db).status.str.match("FAIL").all()


def test_coverage(check: Check, db: duckdb.DuckDBPyConnection):
    check.is_daily("id", pct=0.6)
    df = pd.DataFrame(
        {
            "id": [
                datetime.fromisoformat("2022-01-01") + timedelta(days=i)
                for i in range(1, 10, 2)
            ]
        }
    )
    check.table_name = "df"
    db.register("df", df)
    result = check.validate(db)
    assert result.status.str.match("PASS").all()
    assert result.pass_rate.max() == 0.6
