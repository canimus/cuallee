import pytest

from cuallee.core.check import Check, CheckLevel


def test_positive(snowpark):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_min("ID", 0)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"


def test_negative(snowpark):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_min("ID", 4)
    rs = check.validate(df)
    assert rs.first().STATUS == "FAIL"


@pytest.mark.parametrize("rule_value", [int(0), float(0.0)], ids=["int", "float"])
def test_parameters(snowpark, rule_value):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_min("ID", rule_value)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"


def test_coverage():
    check = Check(CheckLevel.WARNING, "pytest")
    with pytest.raises(TypeError, match="positional arguments"):
        check.has_min("ID", 0, 0.5)
