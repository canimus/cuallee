import pytest


from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from cuallee import Check, CheckLevel


import numpy as np
import pandas as pd#


@pytest.mark.parametrize("column", ['id', 'id'], ids=["int", "float"])
def test_positive(spark, column):
    df = spark.range(10).select(column).withColumn("id2", F.col("id") * 10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_correlation("id", "id2", 1.0)
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().value == "1.0"




def test_positive_correlation(spark: SparkSession):
    check = Check(CheckLevel.WARNING, "PyTestCheck")
    df = spark.range(10).withColumn("id2", F.col("id") * 10)
    check.has_correlation("id", "id2", 1.0)
    test_result_row = check.validate(df).first()
    assert test_result_row.status == "PASS"


def test_no_correlation(spark: SparkSession):
    check = Check(CheckLevel.WARNING, "PyTestCheck")
    df = spark.createDataFrame(
        pd.DataFrame({"id": np.arange(10), "id2": np.random.randn(10)})
    )
    check.has_correlation("id", "id2", 1.0)
    assert check.validate(df).first().status == "FAIL"


def test_correlation_value(spark: SparkSession):
    check = Check(CheckLevel.WARNING, "PyTestCheck")
    df = spark.range(10).withColumn("id2", F.col("id") * 10)
    check.has_correlation("id", "id2", 1.0)
    assert str(check.validate(df).first().value) == "1.0"
