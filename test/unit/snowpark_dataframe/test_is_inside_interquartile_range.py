import pytest
import snowflake.snowpark.functions as F  # type: ignore

from cuallee import Check, CheckLevel


def test_positive(snowpark):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_inside_interquartile_range("ID", pct=0.4)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"


def test_negative(snowpark):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_inside_interquartile_range("ID")
    rs = check.validate(df)
    assert rs.first().STATUS == "FAIL"
    assert rs.first().VIOLATIONS == 4
    assert rs.first().PASS_THRESHOLD == 1.0
    assert rs.first().PASS_RATE == 0.6


@pytest.mark.parametrize(
    "rule_value, value",
    [
        [list([0.25, 0.75]), "(0.25, 0.75)"],
        [tuple([0.25, 0.75]), "(0.25, 0.75)"],
        [list([0.1, 0.8]), "(0.1, 0.8)"],
    ],
    ids=("list", "tuple", "other_values"),
)
def test_parameters(snowpark, rule_value, value):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_inside_interquartile_range("ID", rule_value, pct=0.40)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"
    assert rs.first().VALUE == value


def test_coverage(snowpark):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_inside_interquartile_range("ID", pct=0.4)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"
    assert rs.first().VIOLATIONS == 4
    assert rs.first().PASS_THRESHOLD == 0.4
    assert rs.first().PASS_RATE == 0.6
