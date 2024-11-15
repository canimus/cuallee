import pyspark.sql.functions as F
import pytest

from cuallee.core.check import Check, CheckLevel


def test_positive(spark):
    df = spark.range(10).withColumn("VALUE", F.col("id") + 1000)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_greater_than("VALUE", 999)
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 0
    assert rs.first().pass_threshold == 1.0


def test_negative(spark):
    df = spark.createDataFrame([[1006], [1], [1070], [1900], [10]], ["VALUE"])
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_greater_than("VALUE", 999)
    rs = check.validate(df)
    assert rs.first().status == "FAIL"
    assert rs.first().violations == 2
    assert rs.first().pass_threshold == 1.0
    assert rs.first().pass_rate >= 3 / 5


@pytest.mark.parametrize("rule_value", [int(999), float(999)], ids=("int", "float"))
def test_parameters(spark, rule_value):
    df = spark.range(10).withColumn("VALUE", F.col("id") + 1000)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_greater_than("VALUE", rule_value)
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_coverage(spark):
    df = spark.createDataFrame([[1006], [1], [1070], [1900], [10]], ["VALUE"])
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_greater_than("VALUE", 999, 0.6)
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 2
    assert rs.first().pass_threshold == 0.6
    assert rs.first().pass_rate >= 3 / 5
