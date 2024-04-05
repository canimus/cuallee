import polars as pl

from cuallee import Check


def test_positive(check: Check, db_conn_mysql):
    check.is_on_weekday("id2", pct= 0.70)
    check.table_name = "public.test5"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_negative(check: Check, db_conn_mysql):
    check.is_on_weekday("id2")
    check.table_name = "public.test5"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "FAIL" ).to_series().all()


def test_coverage(check: Check, db_conn_mysql):
    check.is_on_weekday("id2", pct= 7/9)
    check.table_name = "public.test5"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()
    assert (result.select(pl.col("pass_rate")) == 7/9).to_series().all()
