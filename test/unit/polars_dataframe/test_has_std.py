import numpy as np
import polars as pl

from cuallee import Check


def test_positive(check: Check):
    check.has_std("id", 3.0276503540974917)
    df = pl.DataFrame({"id": np.arange(10)})

    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())


def test_negative(check: Check):
    df = pl.DataFrame({"id": [1, 2, 3, 4, 5]})
    check.has_std("id", 10)
    result = check.validate(df).select(pl.col("status")) == "FAIL"
    assert all(result.to_series().to_list())
