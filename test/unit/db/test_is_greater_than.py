import polars as pl

from cuallee import Check


def test_positive(check: Check, postgresql, db_conn):
    check.is_greater_than("id2", 5)
    check.table_name = "public.test1"
    result = check.validate(db_conn)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_negative(check: Check, postgresql, db_conn):
    check.is_greater_than("id", 6)
    check.table_name = "public.test1"
    result = check.validate(db_conn)
    assert (result.select(pl.col("status")) == "FAIL" ).to_series().all()


def test_coverage(check: Check, postgresql, db_conn):
    check.is_greater_than("id5", 2, 2/10)
    check.table_name = "public.test1"
    result = check.validate(db_conn)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()
    assert (result.select(pl.col("pass_rate")) == 2/10).to_series().all()