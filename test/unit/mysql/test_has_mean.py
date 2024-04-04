import pytest
import polars as pl

from cuallee import Check

def test_positive(check: Check, db_conn_mysql):
    check.has_mean("id", 3)
    check.table_name = "public.test1"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()

def test_negative(check: Check, db_conn_mysql):
    check.has_mean("id", 5)
    check.table_name = "public.test1"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "FAIL" ).to_series().all()


@pytest.mark.parametrize("value", [3, 3.0], ids=("int", "float"))
def test_values(check: Check, value, db_conn_mysql):
    check.has_mean("id", value)
    check.table_name = "public.test1"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_coverage(check: Check, db_conn_mysql):
    with pytest.raises(TypeError):
        check.has_mean("id", 5, 0.1)