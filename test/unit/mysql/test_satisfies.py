import polars as pl

from cuallee import Check


def test_positive(check: Check, db_conn_mysql):
    check.satisfies("id", "(id > 0) and (id2 > 99)")
    check.table_name = "public.test2"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_negative(check: Check, db_conn_mysql):
    check.satisfies(("id", "id2"), "(id < 0) and (id2 > 1000)")
    check.table_name = "public.test2"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "FAIL" ).to_series().all()


def test_coverage(check: Check, db_conn_mysql):
    check.satisfies("id", "id > 25", pct=0.5)
    check.table_name = "public.test2"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()
    assert (result.select(pl.col("pass_rate")) == 3/5).to_series().all()
