import pytest

from cuallee import Check, CheckLevel, CustomComputeException
import pyspark.sql.functions as F


def test_positive(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_custom("id", lambda x: x.withColumn("test", F.col("id") >= 0))
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 0
    assert rs.first().pass_threshold == 1.0


def test_negative(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_custom("id", lambda x: x.withColumn("test", F.col("id") >= 5))
    rs = check.validate(df)
    assert rs.first().status == "FAIL"
    assert rs.first().violations == 5
    assert rs.first().pass_threshold == 1.0


def test_parameters(spark):
    df = spark.range(10)
    with pytest.raises(
        CustomComputeException, match="Please provide a Callable/Function for validation"
    ):
        check = Check(CheckLevel.WARNING, "pytest")
        check.is_custom("id", "wrong value")
        check.validate(df)
        
        


def test_coverage(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_custom("id", lambda x: x.withColumn("test", F.col("id") >= 5), 0.4)
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().pass_threshold == 0.4
