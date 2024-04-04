import polars as pl

from cuallee import Check


def test_positive(check: Check, db_conn_mysql):
    check.is_negative("id3")
    check.table_name = "public.test8"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_negative(check: Check, db_conn_mysql):
    check.is_negative("id")
    check.table_name = "public.test8"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "FAIL" ).to_series().all()


def test_integer(check: Check, db_conn_mysql):
    check.is_negative("id3")
    check.table_name = "public.test8"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_float(check: Check, db_conn_mysql):
    check.is_negative("id4")
    check.table_name = "public.test8"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_coverage(check: Check, db_conn_mysql):
    check.is_negative("id5", 0.2)
    check.table_name = "public.test8"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()
    assert (result.select(pl.col("pass_rate")) == 2/5).to_series().all()
