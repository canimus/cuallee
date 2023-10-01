from cuallee import Check
import polars as pl


def test_positive(check: Check):
    df = pl.DataFrame({"id": [1, 2, 3, 4, 5]})
    check.has_max("id", 5)
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())


def test_negative(check: Check):
    df = pl.DataFrame({"id": [1, 2, 3, 4, 5]})
    check.has_max("id", 10)
    result = check.validate(df).select(pl.col("status")) == "FAIL"
    assert all(result.to_series().to_list())
