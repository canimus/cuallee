from cuallee import Check
import polars as pl


def test_positive(check: Check, postgresql, db_conn_psql):
    check.is_inside_interquartile_range("id", pct=0.3)
    check.table_name = "public.test2"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_negative(check: Check, postgresql, db_conn_psql):
    check.is_inside_interquartile_range("id")
    check.table_name = "public.test2"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "FAIL" ).to_series().all()


def test_coverage(check: Check, postgresql, db_conn_psql):
    check.is_inside_interquartile_range("id", pct=3/5)
    check.table_name = "public.test2"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()
    assert (result.select(pl.col("pass_rate")) == 3/5).to_series().all()