import pytest

from cuallee.core.check import Check, CheckLevel


def test_positive(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_mean("id", 4.5)
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_negative(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_mean("id", 3.5)
    rs = check.validate(df)
    assert rs.first().status == "FAIL"


@pytest.mark.parametrize("rule_value", [int(1), float(1.0)], ids=["int", "float"])
def test_parameters(spark, rule_value):
    df = spark.createDataFrame(
        [[1], [1], [1], [1], [1], [1], [1], [1], [1], [1]], ["test"]
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_mean("test", rule_value)
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_coverage():
    check = Check(CheckLevel.WARNING, "pytest")
    with pytest.raises(TypeError, match="positional arguments"):
        check.has_mean("id", 4.5, 0.5)
