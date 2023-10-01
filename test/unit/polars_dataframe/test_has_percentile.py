import polars as pl
import numpy as np
from cuallee import Check
import pytest


def test_positive(check: Check):
    check.has_percentile("id", 6.75, 0.75)
    df = pl.DataFrame({"id": np.arange(10)})
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())


def test_negative(check: Check):
    check.has_percentile("id", 4.75, 0.75)
    df = pl.DataFrame({"id": np.arange(10)})
    result = check.validate(df).select(pl.col("status")) == "FAIL"
    assert all(result.to_series().to_list())


@pytest.mark.parametrize(
    "elements", [[1, 2, 3, 4, 5], [0.1, 0.2, 0.3, 0.4, 0.5]], ids=("int", "float")
)
def test_values(check: Check, elements):
    check.has_percentile("id", elements[2], 0.5)
    df = pl.DataFrame({"id": elements})
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())


def test_coverage(check: Check):
    with pytest.raises(TypeError):
        check.has_percentile("id", 6.75, 0.8, 10000, 0.8)
