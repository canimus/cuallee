import pandas as pd
from cuallee import Check
import pytest
from datetime import datetime, timedelta
import duckdb

def test_positive(check: Check, db: duckdb.DuckDBPyConnection):
    check.is_on_schedule("id", (9, 17))
    df = pd.DataFrame(
        {
            "id": pd.Series(
                [
                    "2022-01-01T09:01:00",
                    "2022-01-01T10:01:00",
                    "2022-01-01T11:01:00",
                    "2022-01-01T12:01:00",
                    "2022-01-01T13:01:00",
                    "2022-01-01T14:01:00",
                    "2022-01-01T15:01:00",
                    "2022-01-01T16:01:00",
                ],
                dtype="datetime64[ns]",
            )
        }
    )
    check.table_name = "df"
    assert check.validate(db).status.str.match("PASS").all()


def test_negative(check: Check, db: duckdb.DuckDBPyConnection):
    check.is_on_schedule("id", (9, 17))
    df = pd.DataFrame(
        {
            "id": pd.Series(
                [
                    "2022-01-01T09:01:00",
                    "2022-01-01T10:01:00",
                    "2022-01-01T11:01:00",
                    "2022-01-01T12:01:00",
                    "2022-01-01T13:01:00",
                    "2022-01-01T14:01:00",
                    "2022-01-01T15:01:00",
                    "2022-01-01T21:01:00",
                ],
                dtype="datetime64[ns]",
            )
        }
    )
    check.table_name = "df"
    assert check.validate(db).status.str.match("FAIL").all()


def test_coverage(check: Check, db: duckdb.DuckDBPyConnection):
    check.is_on_schedule("id", (9, 17), pct=7 / 8)
    df = pd.DataFrame(
        {
            "id": pd.Series(
                [
                    "2022-01-01T09:01:00",
                    "2022-01-01T10:01:00",
                    "2022-01-01T11:01:00",
                    "2022-01-01T12:01:00",
                    "2022-01-01T13:01:00",
                    "2022-01-01T14:01:00",
                    "2022-01-01T15:01:00",
                    "2022-01-01T21:01:00",
                ],
                dtype="datetime64[ns]",
            )
        }
    )
    check.table_name = "df"
    result = check.validate(db)
    assert result.status.str.match("PASS").all()
    assert result.pass_rate.max() == 7 / 8
