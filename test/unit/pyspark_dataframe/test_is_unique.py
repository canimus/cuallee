import pytest
import pyspark.sql.functions as F

from cuallee import Check, CheckLevel


def test_positive(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_unique("id")
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 0.0
    assert rs.first().pass_rate >= 1.0


def test_is_primary_key(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_primary_key("id")
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 0.0
    assert rs.first().pass_rate >= 1.0


def test_negative(spark):
    df = spark.createDataFrame([[0], [2], [2], [3]], ["id"])
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_unique("id")
    rs = check.validate(df)
    assert rs.first().status == "FAIL"
    assert rs.first().violations == 1
    assert rs.first().pass_threshold == 1.0
    assert rs.first().pass_rate >= 3 / 4


def test_parameters(spark):
    return "😅 No parameters to be tested!"


def test_coverage(spark):
    df = spark.createDataFrame([[0], [2], [2], [3]], ["id"])
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_unique("id", 0.7)
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 1
    assert rs.first().pass_threshold == 0.7
    assert rs.first().pass_rate >= 3 / 4
