import polars as pl
from cuallee import Check
import pytest


def test_positive(check: Check):
    check.is_unique("id")
    df = pl.DataFrame({"id": [10, 20], "id2": [300, 500]})
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())

def test_drop_nulls(check: Check):
    check.is_unique("id", ignore_nulls=True)
    df = pl.DataFrame({"id": [10, 20, None, None, 30]})
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())

def test_negative(check: Check):
    check.is_unique("id")
    df = pl.DataFrame({"id": [10, 10], "id2": [300, 500]})
    result = check.validate(df).select(pl.col("status")) == "FAIL"
    assert all(result.to_series().to_list())


def test_coverage(check: Check):
    check.is_unique("id2", 0.5)
    df = pl.DataFrame({"id": [10, 10], "id2": [300, 500]})

    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())
