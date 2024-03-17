import polars as pl
import numpy as np
from cuallee import Check
import pytest


def test_positive(check: Check):
    check.has_infogain("id")
    df = pl.DataFrame({"id": np.arange(10)})
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())


def test_negative(check: Check):
    check.has_infogain("id")
    df = pl.DataFrame({"id": np.ones(10)})
    result = check.validate(df).select(pl.col("status")) == "FAIL"
    assert all(result.to_series().to_list())
