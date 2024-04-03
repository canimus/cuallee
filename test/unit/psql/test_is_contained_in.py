import polars as pl

from cuallee import Check


def test_positive(check: Check, postgresql, db_conn):
    check.is_contained_in("id", [1, 2, 3, 4, 5])
    check.table_name = "public.test1"
    result = check.validate(db_conn)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_negative(check: Check, postgresql, db_conn):
    check.is_contained_in("id", [0, 1, 2, 4, 5])
    check.table_name = "public.test1"
    result = check.validate(db_conn)
    assert (result.select(pl.col("status")) == "FAIL" ).to_series().all()


def test_coverage(check: Check, postgresql, db_conn):
    check.is_contained_in("id5", [1, 2], 8/10)
    check.table_name = "public.test1"
    result = check.validate(db_conn)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()
    assert (result.select(pl.col("pass_rate")) == 8/10).to_series().all()
