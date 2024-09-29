import polars as pl
import pytest

from cuallee import Check


def test_positive(check: Check):
    check.satisfies("id", "(id > 0) and (id2 > 200)")
    df = pl.DataFrame({"id": [10, 20], "id2": [300, 500]})
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())


def test_negative(check: Check):
    check.satisfies(("id", "id2"), "(id < 0) and (id2 > 1000)")
    df = pl.DataFrame({"id": [10, None], "id2": [300, 500]})
    result = check.validate(df).select(pl.col("status")) == "FAIL"
    assert all(result.to_series().to_list())


def test_coverage(check: Check):
    check.satisfies("id", "id > 0", pct=0.5)
    df = pl.DataFrame({"id": [10, -10], "id2": [300, 500]})

    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())
