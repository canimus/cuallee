import polars as pl

from cuallee import Check


def test_positive(check: Check, db_conn_mysql):
    check.is_in_millions("id")
    check.table_name = "public.test7"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_negative(check: Check, db_conn_mysql):
    check.is_in_millions("id2")
    check.table_name = "public.test7"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "FAIL" ).to_series().all()


def test_coverage(check: Check, db_conn_mysql):
    check.is_in_millions("id2", 2/3)
    check.table_name = "public.test7"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()
    assert (result.select(pl.col("pass_rate")) == 2/3).to_series().all()
