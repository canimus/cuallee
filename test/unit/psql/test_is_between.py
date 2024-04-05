import polars as pl

from cuallee import Check


def test_positive(check: Check, postgresql, db_conn_psql):
    check.is_between("id2", (50, 550))
    check.table_name = "public.test2"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_negative(check: Check, postgresql, db_conn_psql):
    check.is_between("id2", (150, 300))
    check.table_name = "public.test2"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "FAIL" ).to_series().all()


def test_dates(check: Check, postgresql, db_conn_psql):
    check.is_between("id2", ('2022-04-01', '2022-08-01'))
    check.table_name = "public.test4"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_numbers(check: Check, postgresql, db_conn_psql):
    check.is_between("id", (10, 60))
    check.table_name = "public.test4"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_coverage(check: Check, postgresql, db_conn_psql):
    check.is_between("id", (5, 35), 6/10)
    check.table_name = "public.test2"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()
    assert (result.select(pl.col("pass_rate")) == 6/10).to_series().all()

