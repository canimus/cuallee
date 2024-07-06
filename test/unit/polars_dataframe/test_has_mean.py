import polars as pl
import numpy as np
from cuallee import Check
import pytest


def test_positive(check: Check):
    check.has_mean("id", 4.5)
    df = pl.DataFrame({"id": np.arange(10)})
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())


def test_negative(check: Check):
    check.has_mean("id", 5)
    df = pl.DataFrame({"id": np.arange(10)})
    result = check.validate(df).select(pl.col("status")) == "FAIL"
    assert all(result.to_series().to_list())


@pytest.mark.parametrize("extra_value", [4, 4.0], ids=("int", "float"))
def test_values(check: Check, extra_value):
    check.has_mean("id", extra_value)
    df = pl.DataFrame({"id": [0, 1, 2, 3, 14] + [extra_value]}, strict=False)
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())


def test_coverage(check: Check):
    with pytest.raises(TypeError):
        check.has_mean("id", 5, 0.1)
