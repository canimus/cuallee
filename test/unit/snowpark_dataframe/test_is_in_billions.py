import pytest
import snowflake.snowpark.functions as F  # type: ignore

from cuallee.core.check import Check, CheckLevel


def test_positive(snowpark):
    df = snowpark.range(10).withColumn("VALUE", F.col("id") + 1e9)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_in_billions("VALUE")
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"
    assert rs.first().VIOLATIONS == 0
    assert rs.first().PASS_THRESHOLD == 1.0


def test_negative(snowpark):
    df = snowpark.createDataFrame([[1e9], [1e8], [5e9], [9e9], [9e8]], ["VALUE"])
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_in_billions("VALUE")
    rs = check.validate(df)
    assert rs.first().STATUS == "FAIL"
    assert rs.first().VIOLATIONS == 2
    assert rs.first().PASS_THRESHOLD == 1.0
    assert rs.first().PASS_RATE == 3 / 5


def test_parameters(snowpark):
    return "😅 No parameters to be tested!"


def test_coverage(snowpark):
    df = snowpark.createDataFrame([[1e9], [1e8], [5e9], [9e9], [9e8]], ["VALUE"])
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_in_billions("VALUE", 0.6)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"
    assert rs.first().VIOLATIONS == 2
    assert rs.first().PASS_THRESHOLD == 0.6
    assert rs.first().PASS_RATE == 3 / 5
