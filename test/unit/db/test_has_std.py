import pytest
import polars as pl

from cuallee import Check


def test_positive(check: Check, postgresql, db_conn):
    check.has_std("id", 1.5811388300841897)
    check.table_name = "public.test1"
    result = check.validate(db_conn)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_negative(check: Check, postgresql, db_conn):
    check.has_std("id", 2)
    check.table_name = "public.test1"
    result = check.validate(db_conn)
    assert (result.select(pl.col("status")) == "FAIL" ).to_series().all()