import pyspark.sql.functions as F

from cuallee import Check, CheckLevel


def test_positive(spark):
    df = spark.range(10).withColumn("value", F.col("id") - 10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_negative("value")
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 0
    assert rs.first().pass_threshold == 1.0


def test_negative(spark):
    df = spark.range(10).withColumn("value", F.col("id") - 8)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_negative("value")
    rs = check.validate(df)
    assert rs.first().status == "FAIL"
    assert rs.first().violations == 2
    assert rs.first().pass_threshold == 1.0
    assert rs.first().pass_rate == 0.8


def test_parameters(spark):
    return "😅 No parameters to be tested!"


def test_coverage(spark):
    df = spark.range(10).withColumn("value", F.col("id") - 8)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_negative("value", 0.8)
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 2
    assert rs.first().pass_threshold == 0.8
    assert rs.first().pass_rate == 4 / 5
