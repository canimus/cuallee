import pytest
import polars as pl

from cuallee import Check

# [ ]: is_daily

@pytest.mark.skip(reason="Not implemented yet!")
def test_positive(check: Check, postgresql, db_conn_psql):
    check.is_daily("id2")
    check.table_name = "public.test5`"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


@pytest.mark.skip(reason="Not implemented yet!")
def test_negative(check: Check, postgresql, db_conn_psql):
    check.is_daily("id3")
    check.table_name = "public.test5`"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "FAIL" ).to_series().all()


@pytest.mark.skip(reason="Not implemented yet!")
def test_coverage(check: Check, postgresql, db_conn_psql):
    check.is_daily("id2", pct=0.2)
    check.table_name = "public.test5"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()
    assert (result.select(pl.col("pass_rate")) == 3/10).to_series().all()
