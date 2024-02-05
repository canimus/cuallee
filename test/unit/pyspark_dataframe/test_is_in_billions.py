import pytest
import pyspark.sql.functions as F

from cuallee import Check, CheckLevel


def test_positive(spark):
    df = spark.range(10).withColumn("VALUE", F.col("id") + 1e9)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_in_billions("VALUE")
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 0
    assert rs.first().pass_threshold == 1.0


def test_negative(spark):
    df = spark.createDataFrame([[1e9], [1e8], [5e9], [9e9], [9e8]], ["VALUE"])
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_in_billions("VALUE")
    rs = check.validate(df)
    assert rs.first().status == "FAIL"
    assert rs.first().violations == 2
    assert rs.first().pass_threshold == 1.0
    assert rs.first().pass_rate >= 3 / 5


def test_parameters(spark):
    return "😅 No parameters to be tested!"


def test_coverage(spark):
    df = spark.createDataFrame([[1e9], [1e8], [5e9], [9e9], [9e8]], ["VALUE"])
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_in_billions("VALUE", 0.6)
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 2
    assert rs.first().pass_threshold == 0.6
    assert rs.first().pass_rate >= 3 / 5
