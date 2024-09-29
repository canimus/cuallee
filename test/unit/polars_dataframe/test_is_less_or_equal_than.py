import polars as pl
import pytest

from cuallee import Check


def test_positive(check: Check):
    check.is_less_or_equal_than("id", 20)
    df = pl.DataFrame({"id": [10, 20], "id2": [300, 500]})
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())


def test_negative(check: Check):
    check.is_less_or_equal_than("id", 19)
    df = pl.DataFrame({"id": [10, 20], "id2": [300, 500]})
    result = check.validate(df).select(pl.col("status")) == "FAIL"
    assert all(result.to_series().to_list())


def test_coverage(check: Check):
    check.is_less_or_equal_than("id", 5, 0.5)
    df = pl.DataFrame({"id": [5, 6], "id2": [300, 500]})

    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())
