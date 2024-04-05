import pytest
import polars as pl

from cuallee import Check

@pytest.mark.skip(reason="Not implemented yet!")
def test_positive(check: Check, db_conn_mysql):
    check.has_percentile("id", 4, 0.75)
    check.table_name = "public.test1"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()

@pytest.mark.skip(reason="Not implemented yet!")
def test_negative(check: Check, db_conn_mysql):
    check.has_percentile("id", 3, 0.75)
    check.table_name = "public.test1"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "FAIL" ).to_series().all()

@pytest.mark.skip(reason="Not implemented yet!")
def test_values_int(check: Check, db_conn_mysql):
    check.has_percentile("id2", 8, 0.5)
    check.table_name = "public.test1"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()

@pytest.mark.skip(reason="Not implemented yet!")
def test_values_float(check: Check, db_conn_mysql):
    check.has_percentile("id3", 30, 0.5)
    check.table_name = "public.test2"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()

@pytest.mark.skip(reason="Not implemented yet!")
def test_coverage(check: Check, db_conn_mysql):
    with pytest.raises(TypeError):
        check.has_percentile("id", 6.75, 0.8, 10000, 0.8)
