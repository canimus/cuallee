import pytest
import snowflake.snowpark.functions as F  # type: ignore

from snowflake.snowpark import Row  # type: ignore
from cuallee import Check, CheckLevel


def test_positive(snowpark):
    df = snowpark.createDataFrame(
        [
            Row(
                name,
            )
            for name in ["seis", "dos", "tres"]
        ],
        schema=["name"],
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_pattern("NAME", r".*s$")
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"


def test_negative(snowpark):
    df = snowpark.createDataFrame(
        [
            Row(
                name,
            )
            for name in ["uno", "dos", "tres"]
        ],
        schema=["name"],
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_pattern("NAME", r".*s$")
    rs = check.validate(df)
    assert rs.first().STATUS == "FAIL"
    assert rs.first().VIOLATIONS == 1, "Uno should violate the expression"


def test_coverage(snowpark):
    df = snowpark.createDataFrame(
        [
            Row(
                name,
            )
            for name in ["uno", "dos", "tres"]
        ],
        schema=["name"],
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_pattern("NAME", r".*s$", 0.65)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"
    assert rs.first().PASS_THRESHOLD == 0.65
    assert rs.first().PASS_RATE == 2 / 3
