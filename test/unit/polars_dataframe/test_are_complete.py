import polars as pl
from cuallee import Check, CheckLevel

def test_positive(check: Check):
    check.are_complete(("id", "id2"))
    df = pl.DataFrame({"id": [10, 20], "id2": [300, 500]})
    result = check.validate(df).select(pl.col('status')) == "PASS"
    assert all(result.to_series().to_list())