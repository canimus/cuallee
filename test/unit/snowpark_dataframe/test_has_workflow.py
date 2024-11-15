import pytest
import snowflake.snowpark.functions as F  # type: ignore

from cuallee.core.check import Check, CheckLevel


def test_positive(snowpark):
    df = snowpark.createDataFrame(
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
    assert rs.first().STATUS == "PASS"


def test_negative(snowpark):
    df = snowpark.createDataFrame(
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
    assert rs.first().STATUS == "FAIL"


@pytest.mark.parametrize(
    "rule_value",
    [
        [tuple(["X", "Y"]), tuple(["Y", "Z"]), tuple(["Z", None])],
        [list(["X", "Y"]), list(["Y", "Z"]), list(["Z", None])],
    ],
    ids=("tuples", "lists"),
)
def test_parameters(snowpark, rule_value):
    df = snowpark.createDataFrame(
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
    assert rs.first().STATUS == "PASS"


def test_coverage(snowpark):
    df = snowpark.createDataFrame(
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
    assert rs.first().STATUS == "PASS"
    assert rs.first().PASS_THRESHOLD == 0.6
    assert rs.first().PASS_RATE == 2 / 3
