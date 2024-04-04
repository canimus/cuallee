import polars as pl

from cuallee import Check


def test_positive(check: Check, db_conn_mysql):
    check.has_cardinality("id", 5)
    check.table_name = "public.test1"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_negative(check: Check, db_conn_mysql):
    check.has_cardinality("id", 3)
    check.table_name = "public.test1"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "FAIL" ).to_series().all()


def test_edge_case(check: Check, db_conn_mysql):
    """A column that have cardinality of 3"""
    check.has_cardinality("id5", 3)
    check.table_name = "public.test1"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()

def test_edge_case_null(check: Check, db_conn_mysql):
    """A column will nulls"""
    check.has_cardinality("id6", 3)
    check.table_name = "public.test1"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()