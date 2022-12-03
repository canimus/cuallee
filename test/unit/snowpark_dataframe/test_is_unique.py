import pytest
import snowflake.snowpark.functions as F  # type: ignore

from cuallee import Check, CheckLevel


def test_positive(snowpark):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_unique("ID")
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"


def test_negative(snowpark):
    df = snowpark.createDataFrame(
        [[0], [2], [2], [3]], ["id"]
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_unique("ID")
    rs = check.validate(df)
    assert rs.first().STATUS == "FAIL"
    assert rs.first().VIOLATIONS == 1
    assert rs.first().PASS_THRESHOLD == 1.0
    assert rs.first().PASS_RATE == 3/4


def test_parameters(snowpark):
    return "😅 No parameters to be tested!"


def test_coverage(snowpark):
    df = snowpark.createDataFrame(
        [[0], [2], [2], [3]], ["id"]
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_unique("ID", 0.7)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"
    assert rs.first().VIOLATIONS == 1
    assert rs.first().PASS_THRESHOLD == 0.7
    assert rs.first().PASS_RATE == 3/4