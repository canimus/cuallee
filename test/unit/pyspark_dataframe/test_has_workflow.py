import pytest
import pyspark.sql.functions as F

from cuallee import Check, CheckLevel


def test_positive(spark):
    df = spark.createDataFrame(
        [
            ["A", "X", 1],
            ["A", "Y", 2],
            ["A", "Z", 3],
            ["B", "X", 1],
            ["B", "Y", 2],
            ["B", "Z", 3],
        ],
        ["GROUP", "EVENT", "ORDER"],
    )
    value = [tuple(["X", "Y"]), tuple(["Y", "Z"]), tuple(["Z", None])]
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_workflow("GROUP", "EVENT", "ORDER", value)
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_negative(spark):
    df = spark.createDataFrame(
        [
            ["A", "X", 1],
            ["A", "Y", 2],
            ["A", "Z", 3],
            ["B", "X", 1],
            ["B", "Z", 2],
            ["B", "Z", 3],
        ],
        ["GROUP", "EVENT", "ORDER"],
    )
    value = [tuple(["X", "Y"]), tuple(["Y", "Z"]), tuple(["Z", None])]
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_workflow("GROUP", "EVENT", "ORDER", value)
    rs = check.validate(df)
    assert rs.first().status == "FAIL"


@pytest.mark.parametrize(
    "rule_value",
    [
        [tuple(["X", "Y"]), tuple(["Y", "Z"]), tuple(["Z", None])],
        [list(["X", "Y"]), list(["Y", "Z"]), list(["Z", None])],
    ],
    ids=("tuples", "lists"),
)
def test_parameters(spark, rule_value):
    df = spark.createDataFrame(
        [
            ["A", "X", 1],
            ["A", "Y", 2],
            ["A", "Z", 3],
            ["B", "X", 1],
            ["B", "Y", 2],
            ["B", "Z", 3],
        ],
        ["GROUP", "EVENT", "ORDER"],
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_workflow("GROUP", "EVENT", "ORDER", rule_value)
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_coverage(spark):
    df = spark.createDataFrame(
        [
            ["A", "X", 1],
            ["A", "Y", 2],
            ["A", "Z", 3],
            ["B", "X", 1],
            ["B", "X", 2],
            ["B", "Z", 3],
        ],
        ["GROUP", "EVENT", "ORDER"],
    )
    value = [tuple(["X", "Y"]), tuple(["Y", "Z"]), tuple(["Z", None])]
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_workflow("GROUP", "EVENT", "ORDER", value, 0.6)
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().pass_threshold == 0.6
    assert rs.first().pass_rate == 2 / 3
