import pyspark
import pyspark.sql.functions as F

from cuallee.core.check import Check, CheckLevel


def test_positive(spark):
    df = spark.range(10).withColumn("VALUE", F.col("id") + 1e6)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_in_millions("VALUE")
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 0
    assert rs.first().pass_threshold == 1.0


def test_negative(spark):
    df = spark.createDataFrame([[1e6], [1e5], [5e7], [9e9], [9e5]], ["VALUE"])
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_in_millions("VALUE")
    rs = check.validate(df)
    assert rs.first().status == "FAIL"
    assert rs.first().violations == 2
    assert rs.first().pass_threshold == 1.0
    assert rs.first().pass_rate >= 3 / 5


def test_parameters(spark):
    return "😅 No parameters to be tested!"


def test_coverage(spark):
    df = spark.createDataFrame([[1e6], [1e5], [5e7], [9e9], [9e5]], ["VALUE"])
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_in_millions("VALUE", 0.6)
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 2
    assert rs.first().pass_threshold == 0.6
    assert rs.first().pass_rate >= 3 / 5
