import pytest
import polars as pl

from cuallee import Check

# [ ]: is_inside_interquartile_range

@pytest.mark.skip(reason="Not implemented yet!")
def test_positive(check: Check, db_conn_mysql):
    check.is_inside_interquartile_range("id", pct=0.3)
    check.table_name = "public.test2"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


@pytest.mark.skip(reason="Not implemented yet!")
def test_negative(check: Check, db_conn_mysql):
    check.is_inside_interquartile_range("id")
    check.table_name = "public.test2"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "FAIL" ).to_series().all()

@pytest.mark.skip(reason="Not implemented yet!")
def test_coverage(check: Check, db_conn_mysql):
    check.is_inside_interquartile_range("id", pct=3/5)
    check.table_name = "public.test2"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()
    assert (result.select(pl.col("pass_rate")) == 3/5).to_series().all()