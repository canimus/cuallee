import polars as pl

from cuallee import Check


def test_positive(check: Check, postgresql, db_conn):
    check.is_positive("id")
    check.table_name = "public.test8"
    result = check.validate(db_conn)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_negative(check: Check, postgresql, db_conn):
    check.is_positive("id3")
    check.table_name = "public.test8"
    result = check.validate(db_conn)
    assert (result.select(pl.col("status")) == "FAIL" ).to_series().all()


def test_integer(check: Check, postgresql, db_conn):
    check.is_positive("id")
    check.table_name = "public.test8"
    result = check.validate(db_conn)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_float(check: Check, postgresql, db_conn):
    check.is_positive("id2")
    check.table_name = "public.test8"
    result = check.validate(db_conn)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_coverage(check: Check, postgresql, db_conn):
    check.is_positive("id5", 0.5)
    check.table_name = "public.test8"
    result = check.validate(db_conn)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()
    assert (result.select(pl.col("pass_rate")) == 3/5).to_series().all()
