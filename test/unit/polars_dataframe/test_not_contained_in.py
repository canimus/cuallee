import polars as pl
from cuallee import Check
import pytest


def test_positive(check: Check):
    check.not_contained_in("id", [10, 20, 30])
    df = pl.DataFrame({"id": range(5)})
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())


def test_negative(check: Check):
    check.not_contained_in("id", [0, 1, 2, 3, 4, 5])
    df = pl.DataFrame({"id": range(5)})
    result = check.validate(df).select(pl.col("status")) == "FAIL"
    assert all(result.to_series().to_list())


def test_coverage(check: Check):
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
    df = pl.DataFrame({"id": range(10)})
    result = check.validate(df)
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())
