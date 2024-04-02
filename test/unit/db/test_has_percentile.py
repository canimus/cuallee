import polars as pl
import numpy as np
from cuallee import Check
import pytest


def test_positive(check: Check, postgresql, db_conn):
    check.has_percentile("id", 4, 0.75)
    check.table_name = "public.test1"
    result = check.validate(db_conn)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_negative(check: Check, postgresql, db_conn):
    check.has_percentile("id", 3, 0.75)
    df = pl.DataFrame({"id": np.arange(10)})
    check.table_name = "public.test1"
    result = check.validate(db_conn)
    assert (result.select(pl.col("status")) == "FAIL" ).to_series().all()


def test_values_int(check: Check, postgresql, db_conn):
    check.has_percentile("id2", 8, 0.5)
    check.table_name = "public.test1"
    result = check.validate(db_conn)
    print(result)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_values_float(check: Check, postgresql, db_conn):
    check.has_percentile("id3", 30, 0.5)
    check.table_name = "public.test2"
    result = check.validate(db_conn)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


def test_coverage(check: Check, postgresql, db_conn):
    with pytest.raises(TypeError):
        check.has_percentile("id", 6.75, 0.8, 10000, 0.8)
