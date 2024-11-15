import pytest
import snowflake.snowpark.functions as F  # type: ignore

from cuallee.core.check import Check, CheckLevel


def test_positive(snowpark):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_sum("ID", 45)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"


def test_negative(snowpark):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_sum("ID", 30)
    rs = check.validate(df)
    assert rs.first().STATUS == "FAIL"


@pytest.mark.parametrize("rule_value", [int(45), float(45.0)], ids=("int", "float"))
def test_parameters(snowpark, rule_value):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_sum("ID", rule_value)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"


def test_coverage():
    check = Check(CheckLevel.WARNING, "pytest")
    with pytest.raises(TypeError, match="positional arguments"):
        check.has_sum("ID", 45, 0.5)
