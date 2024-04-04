import pytest
import polars as pl

from cuallee import Check

@pytest.mark.skip(reason="Not implemented yet!")
def test_positive(check: Check, db_conn_mysql):
    check.has_correlation("id", "id2", 1.0)
    check.table_name = "public.test2"
    result = check.validate(db_conn_mysql)
    print(result)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()

@pytest.mark.skip(reason="Not implemented yet!")
def test_with_null(check: Check, db_conn_mysql):
    check.has_correlation("id4", "id2", 1.0)
    check.table_name = "public.test2"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()

@pytest.mark.skip(reason="Not implemented yet!")
def test_values(check: Check, db_conn_mysql):
    check.has_correlation("id", "id3", 1.0)
    check.table_name = "public.test2"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()

@pytest.mark.skip(reason="Not implemented yet!")
def test_coverage(check: Check):
    with pytest.raises(TypeError, match="positional arguments"):
        check.has_correlation("id", "id2", 1.0, 0.75)
