import polars as pl

from cuallee import Check


def test_positive(check: Check):
    df = pl.DataFrame({"id": ["Herminio", "Herbert", "Harry"]})
    check.has_pattern("id", r"^H.*")
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())

def test_is_legit(check: Check):
    df = pl.DataFrame({"id": ["Herminio", "Herbert", "Harry"]})
    check.is_legit("id")
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())

def test_is_not_legit(check: Check):
    df = pl.DataFrame({"id": ["Herminio", "Herbert", ""]})
    check.is_legit("id")
    result = check.validate(df).select(pl.col("status")) == "FAIL"
    assert all(result.to_series().to_list())
