import pytest

from cuallee.core.check import Check, CheckLevel


def test_positive(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_max("id", 9)
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_negative(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_max("id", 4)
    rs = check.validate(df)
    assert rs.first().status == "FAIL"


@pytest.mark.parametrize("rule_value", [int(9), float(9.0)], ids=["int", "float"])
def test_parameters(spark, rule_value):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_max("id", rule_value)
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_coverage():
    check = Check(CheckLevel.WARNING, "pytest")
    with pytest.raises(TypeError, match="positional arguments"):
        check.has_max("id", 9, 0.5)
