import pytest
import pyspark.sql.functions as F

from cuallee import Check, CheckLevel


def test_positive(spark):
    df = spark.range(10).withColumn("VALUE", 1000 - F.col("id"))
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_less_or_equal_than("VALUE", 1000)
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 0
    assert rs.first().pass_threshold == 1.0


def test_negative(spark):
    df = spark.createDataFrame([[1060], [1], [1073], [1000], [10]], ["VALUE"])
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_less_or_equal_than("VALUE", 1000)
    rs = check.validate(df)
    assert rs.first().status == "FAIL"
    assert rs.first().violations == 2
    assert rs.first().pass_threshold == 1.0
    assert rs.first().pass_rate == 3 / 5


@pytest.mark.parametrize("rule_value", [int(1000), float(1000)], ids=("int", "float"))
def test_parameters(spark, rule_value):
    df = spark.range(10).withColumn("VALUE", 1000 - F.col("id"))
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_less_or_equal_than("VALUE", rule_value)
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_coverage(spark):
    df = spark.createDataFrame([[1060], [1], [1073], [1000], [10]], ["VALUE"])
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_less_or_equal_than("VALUE", 1000, 0.6)
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 2
    assert rs.first().pass_threshold == 0.6
    assert rs.first().pass_rate == 3 / 5
