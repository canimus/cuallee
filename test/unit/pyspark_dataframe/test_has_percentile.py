import pyspark.sql.functions as F
import pytest

from cuallee import Check, CheckLevel


def test_positive(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_percentile("id", 4.0, 0.5)
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_negative(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_percentile("id", 4.5, 0.5)
    rs = check.validate(df)
    assert rs.first().status == "FAIL"


@pytest.mark.parametrize(
    "data_type, rule_value",
    [
        [[[0], [2], [4]], int(2)],
        [[[0], [2], [4]], float(2.0)],
        [[[float(0.0)], [float(2.0)], [float(4.0)]], int(2)],
        [[[float(0.0)], [float(2.0)], [float(4.0)]], float(2.0)],
    ],
    ids=["value_int", "value_float", "data_int", "data_float"],
)
def test_parameters(spark, data_type, rule_value):
    df = spark.createDataFrame(data_type, ["TEST"])
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_percentile("TEST", rule_value, 0.5)
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_coverage():
    check = Check(CheckLevel.WARNING, "pytest")
    with pytest.raises(TypeError, match="positional arguments"):
        check.has_percentile("id", 3.0, 0.5, 1000, 0.7)


@pytest.mark.parametrize(
    "rule_value, percentage, precision, expected",
    [[4.0, 0.5, 1000, "PASS"], [9.0, 1.0, 1000, "PASS"], [4.0, 0.5, 100000000, "PASS"]],
    ids=["percentage_pass", "change_percentage", "precision"],
)
def test_settings(spark, rule_value, percentage, precision, expected):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_percentile("id", rule_value, percentage, precision)
    rs = check.validate(df)
    assert rs.first().status == expected
