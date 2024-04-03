import polars as pl

from cuallee import Check


def test_positive(check: Check, postgresql, db_conn_psql):
    check.is_on_schedule("id2", (9, 17))
    check.table_name = "public.test6"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()



def test_negative(check: Check, postgresql, db_conn_psql):
    check.is_on_schedule("id2", (16, 20))
    check.table_name = "public.test6"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "FAIL" ).to_series().all()


def test_coverage(check: Check, postgresql, db_conn_psql):
    check.is_on_schedule("id2", (12, 17), pct=3/5)
    check.table_name = "public.test6"
    result = check.validate(db_conn_psql)
    print(result)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()
    assert (result.select(pl.col("pass_rate")) == 3/5).to_series().all()

