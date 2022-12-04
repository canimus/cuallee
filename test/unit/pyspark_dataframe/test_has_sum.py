import pytest
import pyspark.sql.functions as F  # type: ignore

from cuallee import Check, CheckLevel


def test_positive(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_sum("id", 45)
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_negative(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_sum("id", 30)
    rs = check.validate(df)
    assert rs.first().status == "FAIL"


@pytest.mark.parametrize("rule_value", [int(45), float(45.0)], ids=("int", "float"))
def test_parameters(spark, rule_value):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_sum("id", rule_value)
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_coverage():
    check = Check(CheckLevel.WARNING, "pytest")
    with pytest.raises(TypeError, match="positional arguments"):
        check.has_sum("id", 45, 0.5)
