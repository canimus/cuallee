import polars as pl

from cuallee import Check

def test_positive(check: Check, postgresql, db_conn_psql):
    check.has_pattern("id", r"^H.*")
    check.table_name = "public.test9"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_is_legit(check: Check, postgresql, db_conn_psql):
    check.has_pattern("id3", r"^H.*")
    check.table_name = "public.test9"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "FAIL" ).to_series().all()


def test_is_not_legit(check: Check, postgresql, db_conn_psql):
    check.has_pattern("id2", r"^H.*", 2/3)
    check.table_name = "public.test9"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()
    assert (result.select(pl.col("pass_rate")) == 2/3).to_series().all()
