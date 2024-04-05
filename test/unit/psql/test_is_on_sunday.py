import polars as pl

from cuallee import Check


def test_positive(check: Check, postgresql, db_conn_psql):
    check.is_on_sunday("id2", pct= 0.10)
    check.table_name = "public.test5"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_negative(check: Check, postgresql, db_conn_psql):
    check.is_on_sunday("id2")
    check.table_name = "public.test5"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "FAIL" ).to_series().all()


def test_coverage(check: Check, postgresql, db_conn_psql):
    check.is_on_sunday("id2", pct= 1/9)
    check.table_name = "public.test5"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()
    assert (result.select(pl.col("pass_rate")) == 1/9).to_series().all()