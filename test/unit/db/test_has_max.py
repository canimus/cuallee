import polars as pl

from cuallee import Check


def test_positive(check: Check, postgresql, db_conn):
    check.has_max("id", 5)
    check.table_name = "public.test1"
    result = check.validate(db_conn)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()

def test_negative(check: Check, postgresql, db_conn):
    check.has_max("id", 10)
    check.table_name = "public.test1"
    result = check.validate(db_conn)
    assert (result.select(pl.col("status")) == "FAIL" ).to_series().all()