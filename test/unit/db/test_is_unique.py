import polars as pl

from cuallee import Check


def test_positive(check: Check, postgresql, db_conn):
    check.is_unique("id")
    check.table_name = "public.test1"
    result = check.validate(db_conn).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())


def test_negative(check: Check, postgresql, db_conn):
    check.is_unique("id2")
    check.table_name = "public.test1"
    result = check.validate(db_conn).select(pl.col("status")) == "FAIL"
    assert all(result.to_series().to_list())


def test_coverage(check: Check, postgresql, db_conn):
    check.is_unique("id3", 0.5)
    check.table_name = "public.test1"
    result = check.validate(db_conn).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())
