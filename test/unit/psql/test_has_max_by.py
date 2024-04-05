import pytest
import polars as pl

from cuallee import Check

def test_positive(check: Check, postgresql, db_conn_psql):
    check.has_max_by("id", "id2", 10)
    check.table_name = "public.test1"
    result = check.validate(db_conn_psql)
    print(result)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_negative(check: Check, postgresql, db_conn_psql):
    check.has_max_by("id", "id2", 7)
    check.table_name = "public.test1"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "FAIL" ).to_series().all()


def test_values_string(check: Check, postgresql, db_conn_psql):
    check.has_max_by("id", "id4", 10)
    check.table_name = "public.test9"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_coverage(check: Check):
    with pytest.raises(TypeError):
        check.has_max_by("id", "id2", 20, 100)
