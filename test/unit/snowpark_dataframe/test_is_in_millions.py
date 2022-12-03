import pyspark
import snowflake.snowpark.functions as F  # type: ignore

from cuallee import Check, CheckLevel


def test_positive(snowpark):
    df = snowpark.range(10).withColumn("VALUE", F.col("id") + 1e6)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_in_millions("VALUE")
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"
    assert rs.first().VIOLATIONS == 0
    assert rs.first().PASS_THRESHOLD == 1.0


def test_negative(snowpark):
    df = snowpark.createDataFrame(
        [[1e6], [1e5], [5e7], [9e9], [9e5]], ["VALUE"]
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_in_millions("VALUE")
    rs = check.validate(df)
    assert rs.first().STATUS == "FAIL"
    assert rs.first().VIOLATIONS == 2
    assert rs.first().PASS_THRESHOLD == 1.0
    assert rs.first().PASS_RATE == 3/5


def test_parameters(snowpark):
    return "😅 No parameters to be tested!"


def test_coverage(snowpark):
    df = snowpark.createDataFrame(
        [[1e6], [1e5], [5e7], [9e9], [9e5]], ["VALUE"]
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_in_millions("VALUE", 0.6)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"
    assert rs.first().VIOLATIONS == 2
    assert rs.first().PASS_THRESHOLD == 0.6
    assert rs.first().PASS_RATE == 3/5
