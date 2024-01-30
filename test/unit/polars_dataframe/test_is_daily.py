import polars as pl
from cuallee import Check
import pytest
from datetime import datetime, timedelta
from datetime import date


def test_positive(check: Check):
    check.is_daily("id")
    df = pl.DataFrame(
        {
            "id": pl.date_range(
                start=date(2022, 11, 20),
                end=date(2022, 11, 26),
                interval="1d",
                eager=True,
            )
        }
    )

    assert check.validate(df).select(pl.col("status").eq("PASS").all()).item()


def test_negative(check: Check):
    check.is_daily("id")
    df = pl.DataFrame(
        {"id": [datetime.today() + timedelta(days=i) for i in range(1, 10, 2)]}
    )
    assert check.validate(df).select(pl.col("status").eq("FAIL").all()).item()


def test_coverage(check: Check):
    check.is_daily("id", pct=0.6)
    df = pl.DataFrame(
        {"id": [datetime(2022, 12, 12) + timedelta(days=i) for i in range(1, 10, 2)]}
    )

    assert check.validate(df).select(pl.col("status").eq("PASS").all()).item()
