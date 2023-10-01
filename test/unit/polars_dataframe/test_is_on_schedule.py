import polars as pl
from cuallee import Check
import pytest
from datetime import datetime, timedelta


def test_positive(check: Check):
    check.is_on_schedule("id", (9, 17))
    df = pl.DataFrame({"id": pl.Series([datetime(2022,1,1,i,1,0) for i in range(9,18)], dtype=pl.Datetime)})
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())


def test_negative(check: Check):
    check.is_on_schedule("id", (9, 17))
    df = pl.DataFrame({"id": pl.Series([datetime(2022,1,1,i,1,0) for i in range(9,21)], dtype=pl.Datetime)})

    result = check.validate(df).select(pl.col("status")) == "FAIL"
    assert all(result.to_series().to_list())


def test_coverage(check: Check):
    check.is_on_schedule("id", (9, 17), pct=7 / 8)
    df = pl.DataFrame({"id": pl.Series([datetime(2022,1,1,i,1,0) for i in range(9,19)], dtype=pl.Datetime)})
    result = check.validate(df)
    
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())
