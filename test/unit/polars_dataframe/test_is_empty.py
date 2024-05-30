import polars as pl
from cuallee import Check
import pytest


def test_positive(check: Check):
    check.is_empty("id")
    df = pl.DataFrame({"id": [None, None], "id2": [None, None]})
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())


def test_negative(check: Check):
    check.is_empty("id")
    df = pl.DataFrame({"id": [10, None], "id2": [300, 500]})
    result = check.validate(df).select(pl.col("status")) == "FAIL"
    assert all(result.to_series().to_list())


def test_coverage(check: Check):
    check.is_empty("id2", 0.5)
    df = pl.DataFrame({"id": [10, None], "id2": [300, 500]})

    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())
