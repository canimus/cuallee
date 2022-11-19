import pytest

from cuallee import Check, CheckLevel


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
