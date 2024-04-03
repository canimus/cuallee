import polars as pl

from cuallee import Check


def test_positive(check: Check, postgresql, db_conn):
    check.is_negative("id3")
    check.table_name = "public.test8"
    result = check.validate(db_conn)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_negative(check: Check, postgresql, db_conn):
    check.is_negative("id")
    check.table_name = "public.test8"
    result = check.validate(db_conn)
    assert (result.select(pl.col("status")) == "FAIL" ).to_series().all()


def test_integer(check: Check, postgresql, db_conn):
    check.is_negative("id3")
    check.table_name = "public.test8"
    result = check.validate(db_conn)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_float(check: Check, postgresql, db_conn):
    check.is_negative("id4")
    check.table_name = "public.test8"
    result = check.validate(db_conn)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_coverage(check: Check, postgresql, db_conn):
    check.is_negative("id5", 0.2)
    check.table_name = "public.test8"
    result = check.validate(db_conn)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()
    assert (result.select(pl.col("pass_rate")) == 2/5).to_series().all()
