import pytest
from cuallee import Check
import pandas as pd
import polars as pl
import duckdb as dk
from pyspark.sql import SparkSession


def test_pandas_ok(check: Check):
    df = pd.DataFrame({"A": range(10)})
    check.is_complete("A")
    assert check.ok(df)


def test_polars_ok(check: Check):
    df = pl.DataFrame({"A": range(10)})
    check.is_complete("A")
    assert check.ok(df)


def test_pyspark_ok(check: Check, spark: SparkSession):
    df = spark.range(10)
    check.is_complete("id")
    assert check.ok(df)
