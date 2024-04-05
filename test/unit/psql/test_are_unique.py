import pytest
import polars as pl
from cuallee import Check


def test_positive(check: Check, postgresql, db_conn_psql):
    check.are_unique(("id", "id2"))
    check.table_name = "public.test1"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_negative(check: Check, postgresql, db_conn_psql):
    check.are_unique(("id3", "id4"))
    check.table_name = "public.test1"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "FAIL" ).to_series().all()


@pytest.mark.parametrize(
    "rule_column", [tuple(["id3", "id4"]), list(["id3", "id4"])], ids=("tuple", "list")
)
def test_parameters(check: Check, postgresql, db_conn_psql, rule_column):
    check.are_unique(rule_column)
    check.table_name = "public.test1"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "FAIL" ).to_series().all()


def test_coverage(check: Check, postgresql, db_conn_psql):
    check.are_unique(("id", "id5"), 0.5 )
    check.table_name = "public.test1"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()
    assert (result.select(pl.col("pass_rate")) == 8/10).to_series().all()

