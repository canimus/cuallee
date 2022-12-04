import pytest

from cuallee import Check, CheckLevel


def test_positive(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_min("id", 0)
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_negative(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_min("id", 4)
    rs = check.validate(df)
    assert rs.first().status == "FAIL"


@pytest.mark.parametrize("rule_value", [int(0), float(0.0)], ids=["int", "float"])
def test_parameters(spark, rule_value):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_min("id", rule_value)
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_coverage():
    check = Check(CheckLevel.WARNING, "pytest")
    with pytest.raises(TypeError, match="positional arguments"):
        check.has_min("id", 0, 0.5)
