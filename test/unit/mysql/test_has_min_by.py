import pytest
import polars as pl

from cuallee import Check


def test_positive(check: Check, db_conn_mysql):
    check.has_min_by("id", "id2", 6)
    check.table_name = "public.test1"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_negative(check: Check, db_conn_mysql):
    check.has_min_by("id", "id2", 12)
    check.table_name = "public.test1"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "FAIL" ).to_series().all()


def test_values_string(check: Check, db_conn_mysql):
    check.has_min_by("id", "id4", 30)
    check.table_name = "public.test9"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_coverage(check: Check, db_conn_mysql):
    with pytest.raises(TypeError):
        check.has_min_by("id2", "id", 20, 100)
