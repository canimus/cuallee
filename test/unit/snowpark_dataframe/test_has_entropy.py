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
def test_positive(snowpark, data):
    df = snowpark.createDataFrame(data, ["test"])
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_entropy("TEST", 1.0, 0.1)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"
    assert rs.first().VALUE == "1.0"


def test_negative(snowpark):
    df = snowpark.createDataFrame(
        [[0], [0], [0], [0], [0], [1], [0], [1], [0], [1]], ["test"]
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_entropy("TEST", 1.0, 0.1)
    rs = check.validate(df)
    assert rs.first().STATUS == "FAIL"


@pytest.mark.parametrize("rule_value", [int(1), float(1.0)], ids=["int", "float"])
def test_parameters(snowpark, rule_value):
    df = snowpark.createDataFrame(
        [[0], [0], [0], [0], [0], [1], [1], [1], [1], [1]], ["test"]
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_entropy("TEST", rule_value, 0.1)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"


def test_coverage():
    check = Check(CheckLevel.WARNING, "pytest")
    with pytest.raises(TypeError, match="positional arguments"):
        check.has_correlation("ID", "ID2", 1.0, 0.5)


@pytest.mark.parametrize(
    "tolerance, expected",
    [[1, "PASS"], [1.0, "PASS"], [0.00001, "PASS"]],
    ids=["int", "float", "low_tolerance"],
)
def test_settings(snowpark, tolerance, expected):  # tolerance
    df = snowpark.createDataFrame(
        [[0], [0], [0], [0], [0], [1], [1], [1], [1], [1]], ["test"]
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_entropy("TEST", 1.0, tolerance)
    rs = check.validate(df)
    assert rs.first().STATUS == expected
