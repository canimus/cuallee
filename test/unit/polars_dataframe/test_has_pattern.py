import polars as pl

from cuallee import Check


def test_positive(check: Check):
    df = pl.DataFrame({"id" : ["Herminio", "Herbert", "Harry"]})
    check.has_pattern("id", r"^H.*")
    result = check.validate(df).select(pl.col('status')) == "PASS"
    assert all(result.to_series().to_list())