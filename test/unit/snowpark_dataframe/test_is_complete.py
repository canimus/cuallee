import pytest

from cuallee import Check, CheckLevel


def test_positive(snowpark):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_complete("ID")
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"
    assert rs.first().VIOLATIONS == 0
    assert rs.first().PASS_THRESHOLD == 1.0


@pytest.mark.parametrize(
    "data, violation, pass_rate",
    [
        [[[0], [1], [None], [4], [5]], 1, 4 / 5],
        [[[0], [1], [None], [4], [None]], 2, 3 / 5],
    ],
    ids=("one_null_value", "two_null_value"),
)
def test_negative(snowpark, data, violation, pass_rate):
    df = snowpark.createDataFrame(data, ["ID"])
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_complete("ID")
    rs = check.validate(df)
    assert rs.first().STATUS == "FAIL"
    assert rs.first().VIOLATIONS == violation
    assert rs.first().PASS_THRESHOLD == 1.0
    assert rs.first().PASS_RATE == pass_rate


def test_parameters():
    return "😅 No parameters to be tested!"


def test_coverage(snowpark):
    df = snowpark.createDataFrame([[0], [1], [None], [4], [5]], ["ID"])
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_complete("ID", 0.7)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"
    assert rs.first().PASS_THRESHOLD == 0.7
    assert rs.first().PASS_RATE == 4 / 5
