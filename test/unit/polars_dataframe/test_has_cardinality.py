import numpy as np
import polars as pl
import pytest

from cuallee import Check


def test_positive(check: Check):
    check.has_cardinality("id", 10)
    df = pl.DataFrame({"id": np.arange(10)})
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())


def test_negative(check: Check):
    check.has_cardinality("id", 5)
    df = pl.DataFrame({"id": np.arange(10)})
    result = check.validate(df).select(pl.col("status")) == "FAIL"
    assert all(result.to_series().to_list())
