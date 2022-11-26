import pandas as pd
import numpy as np
from cuallee import Check
import pytest
from pyspark.sql import SparkSession


def test_positive(spark: SparkSession, check: Check):
    check.has_sum("id", 45)
    df = spark.range(10)
    assert check.validate(df).first().status == "PASS"


def test_negative(spark: SparkSession, check: Check):
    check.has_sum("id", 5)
    df = spark.range(10)
    assert check.validate(df).first().status == "FAIL"


def test_coverage(spark: SparkSession, check: Check):
    with pytest.raises(TypeError):
        check.has_sum("id", 5, 0.1)
