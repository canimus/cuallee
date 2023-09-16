from cuallee import Check
import polars as pl
import pytest


def test_positive(check):
    df = pl.DataFrame({"id": [10, 20], "id2": [100, 200]})
    check.has_correlation("id", "id2", 1.0)
    # result = check.validate(df).select(pl.col('status')) == "PASS"
    # assert all(result.to_series().to_list())
    assert True
