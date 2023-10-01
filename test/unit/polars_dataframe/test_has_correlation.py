import pytest

from cuallee import Check
import polars as pl
import numpy as np


def test_positive(check: Check):
    check.has_correlation("id", "id2", 1.0)
    df = pl.DataFrame({"id": [10, 20], "id2": [100, 200]})
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())


def test_negative(check: Check):
    check.has_correlation("id", "id2", 1.0)
    df = pl.DataFrame({"id": [10, 20, 30, None], "id2": [100, 200, 300, 400]})
    result = check.validate(df).select(pl.col("status")) == "FAIL"
    assert all(result.to_series().to_list())


def test_values(check: Check):
    check.has_correlation("id", "id2", 1.0)
    df = pl.DataFrame({"id": [1, 2, 3], "id2": [1.0, 2.0, 3.0]})
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())


def test_coverage(check: Check):
    with pytest.raises(TypeError, match="positional arguments"):
        check.has_correlation("id", "id2", 1.0, 0.75)
