import pytest

from cuallee.core.check import Check, CheckLevel


@pytest.mark.parametrize(
    "data",
    [
        [[0], [0], [0], [0], [0], [1], [1], [1], [1], [1]],
        [[0.0], [0.0], [0.0], [0.0], [0.0], [1.0], [1.0], [1.0], [1.0], [1.0]],
    ],
    ids=["int", "float"],
)
def test_positive(spark, data):
    df = spark.createDataFrame(data, ["test"])
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_entropy("test", 1.0, 0.1)
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().value == "1.0"


def test_negative(spark):
    df = spark.createDataFrame(
        [[0], [0], [0], [0], [0], [1], [0], [1], [0], [1]], ["test"]
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_entropy("test", 1.0, 0.1)
    rs = check.validate(df)
    assert rs.first().status == "FAIL"


@pytest.mark.parametrize("rule_value", [int(1), float(1.0)], ids=["int", "float"])
def test_parameters(spark, rule_value):
    df = spark.createDataFrame(
        [[0], [0], [0], [0], [0], [1], [1], [1], [1], [1]], ["test"]
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_entropy("test", rule_value, 0.1)
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_coverage():
    check = Check(CheckLevel.WARNING, "pytest")
    with pytest.raises(TypeError, match="positional arguments"):
        check.has_correlation("id", "id2", 1.0, 0.5)


@pytest.mark.parametrize(
    "tolerance, expected",
    [[1, "PASS"], [1.0, "PASS"], [0.00001, "PASS"]],
    ids=["int", "float", "low_tolerance"],
)
def test_settings(spark, tolerance, expected):  # tolerance
    df = spark.createDataFrame(
        [[0], [0], [0], [0], [0], [1], [1], [1], [1], [1]], ["test"]
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_entropy("test", 1.0, tolerance)
    rs = check.validate(df)
    assert rs.first().status == expected
