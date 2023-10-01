import polars as pl
from cuallee import Check
import pendulum as lu
from datetime import date

def test_positive(check: Check):
    check.is_on_weekend("id")
    df = pl.DataFrame(
        {"id": pl.Series([lu.now().next(lu.SATURDAY).date()], dtype=pl.Date)}
    )
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())


def test_negative(check: Check):
    check.is_on_weekend("id")
    df = pl.DataFrame(
        {"id": pl.Series([lu.now().next(lu.FRIDAY).date()], dtype=pl.Date)}
    )
    result = check.validate(df).select(pl.col("status")) == "FAIL"
    assert all(result.to_series().to_list())


def test_coverage(check: Check):
    check.is_on_weekend("id", pct=2 / 7)
    df = pl.DataFrame({
        "id": pl.date_range(start=date(2022,11,20), end=date(2022,11,26), interval="1d", eager=True)
    })
    
    result = check.validate(df)
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())
