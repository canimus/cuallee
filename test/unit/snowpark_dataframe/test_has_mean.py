import pytest

from cuallee.core.check import Check, CheckLevel


def test_positive(snowpark):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_mean("ID", 4.5)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"


def test_negative(snowpark):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_mean("ID", 3.5)
    rs = check.validate(df)
    assert rs.first().STATUS == "FAIL"


@pytest.mark.parametrize("rule_value", [int(1), float(1.0)], ids=["int", "float"])
def test_parameters(snowpark, rule_value):
    df = snowpark.createDataFrame(
        [[1], [1], [1], [1], [1], [1], [1], [1], [1], [1]], ["test"]
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_mean("TEST", rule_value)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"


def test_coverage():
    check = Check(CheckLevel.WARNING, "pytest")
    with pytest.raises(TypeError, match="positional arguments"):
        check.has_mean("ID", 4.5, 0.5)
