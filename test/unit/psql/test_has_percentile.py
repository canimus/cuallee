import pytest
import polars as pl

from cuallee import Check


def test_positive(check: Check, postgresql, db_conn_psql):
    check.has_percentile("id", 4, 0.75)
    check.table_name = "public.test1"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_negative(check: Check, postgresql, db_conn_psql):
    check.has_percentile("id", 3, 0.75)
    check.table_name = "public.test1"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "FAIL" ).to_series().all()


def test_values_int(check: Check, postgresql, db_conn_psql):
    check.has_percentile("id2", 8, 0.5)
    check.table_name = "public.test1"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_values_float(check: Check, postgresql, db_conn_psql):
    check.has_percentile("id3", 30, 0.5)
    check.table_name = "public.test2"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_coverage(check: Check, postgresql, db_conn_psql):
    with pytest.raises(TypeError):
        check.has_percentile("id", 6.75, 0.8, 10000, 0.8)
