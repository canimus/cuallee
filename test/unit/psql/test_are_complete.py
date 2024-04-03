import pytest
import polars as pl

from cuallee import Check


def test_positive(check: Check, postgresql, db_conn_psql):
    check.are_complete(("id", "id2"))
    check.table_name = "public.test1"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_negative(check: Check, postgresql, db_conn_psql):
    check.are_complete(("id", "id6"))
    check.table_name = "public.test1"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "FAIL" ).to_series().all()


@pytest.mark.parametrize(
    "rule_column", [tuple(["id", "id6"]), list(["id", "id6"])], ids=("tuple", "list")
)
def test_parameters(rule_column, check: Check, postgresql, db_conn_psql):
    check.are_complete(rule_column)
    check.table_name = "public.test1"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "FAIL" ).to_series().all()


def test_coverage(check: Check, postgresql, db_conn_psql):
    check.are_complete(("id", "id6"), 0.75)
    check.table_name = "public.test1"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()
    assert (result.select(pl.col("pass_rate")) == 8/10).to_series().all()
