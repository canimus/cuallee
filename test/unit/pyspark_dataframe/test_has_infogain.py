import pandas as pd
import pyspark.sql.functions as F  # type: ignore
import pytest

from cuallee import Check, CheckLevel


def test_positive(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_infogain("id")
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_negative(spark):
    df = spark.createDataFrame([(0,), (0,), (0,), (0,), (0,)], schema="id integer")
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_infogain("id")
    rs = check.validate(df)
    assert rs.first().status == "FAIL"


@pytest.mark.parametrize(
    ("rule_type", "rule_value"),
    [
        pytest.param("int", [1, 2, 3], id="numeric"),
        pytest.param("string", ["A", "B", "C"], id="categorical"),
    ],
)
def test_parameters(spark, rule_type, rule_value):
    df = spark.createDataFrame(pd.DataFrame(rule_value, columns=["id"]))
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_infogain("id")
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_coverage():
    check = Check(CheckLevel.WARNING, "pytest")
    with pytest.raises(TypeError, match="positional arguments"):
        check.has_infogain("id", 45, 0.5)
