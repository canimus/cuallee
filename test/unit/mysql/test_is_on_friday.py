import polars as pl

from cuallee import Check


def test_positive(check: Check, db_conn_mysql):
    check.is_on_friday("id2", pct= 0.10)
    check.table_name = "public.test5"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_negative(check: Check, db_conn_mysql):
    check.is_on_friday("id2")
    check.table_name = "public.test5"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "FAIL" ).to_series().all()


def test_coverage(check: Check, db_conn_mysql):
    check.is_on_friday("id2", pct= 1/9)
    check.table_name = "public.test5"
    result = check.validate(db_conn_mysql)
    print(result)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()
    assert (result.select(pl.col("pass_rate")) == 1/9).to_series().all()
