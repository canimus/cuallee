import snowflake.snowpark.functions as F  # type: ignore

from snowflake.snowpark import DataFrame  # type: ignore
from cuallee import Check, CheckLevel


def test_positive(snowpark):
    df = snowpark.range(10).withColumn("value", F.col("id") - 10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_negative("VALUE")
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"
    assert rs.first().VIOLATIONS == 0
    assert rs.first().PASS_THRESHOLD == 1.0


def test_negative(snowpark):
    df = snowpark.range(10).withColumn("value", F.col("id") - 8)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_negative("VALUE")
    rs = check.validate(df)
    assert rs.first().STATUS == "FAIL"
    assert rs.first().VIOLATIONS == 2
    assert rs.first().PASS_THRESHOLD == 1.0
    assert rs.first().PASS_RATE == 0.8


def test_parameters(snowpark):
    return "😅 No parameters to be tested!"


def test_coverage(snowpark):
    df = snowpark.range(10).withColumn("value", F.col("id") - 8)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_negative("VALUE", 0.8)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"
    assert rs.first().VIOLATIONS == 2
    assert rs.first().PASS_THRESHOLD == 0.8
    assert rs.first().PASS_RATE == 4 / 5
