import pytest

from datetime import datetime, date
from cuallee import Check, CheckLevel


def test_negative(snowpark):
    df = snowpark.createDataFrame(
        [[1, "blue"], [2, "green"], [3, "grey"]], ["id", "desc"]
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.not_contained_in("DESC", ("blue", "red", "green", "grey", "black"))
    rs = check.validate(df)
    assert rs.first().STATUS == "FAIL"
    assert rs.first().VIOLATIONS == 3
    assert rs.first().PASS_THRESHOLD == 1.0


def test_positive(snowpark):
    df = snowpark.createDataFrame(
        [[1, "blue"], [2, "green"], [3, "grey"]], ["id", "desc"]
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.not_contained_in("DESC", ("purple", "red"))
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"
    assert rs.first().VIOLATIONS == 0
    assert rs.first().PASS_THRESHOLD == 1.0


@pytest.mark.parametrize(
    "data, columns, rule_value",
    [
        [
            [[1, "blue"], [2, "green"], [3, "grey"]],
            ["id", "test_col"],
            tuple(["blue", "green", "grey"]),
        ],
        [
            [[1, "blue"], [2, "green"], [3, "grey"]],
            ["id", "test_col"],
            list(["blue", "green", "grey"]),
        ],
        [[[1, 10], [2, 15], [3, 17]], ["id", "test_col"], (10, 15, 17)],
        [
            [[1, 10], [2, 15], [3, 17]],
            ["id", "test_col"],
            (float(10.0), float(15.0), float(17.0)),
        ],
        [
            [[1, float(10)], [2, float(15)], [3, float(17)]],
            ["id", "test_col"],
            (10, 15, 17),
        ],
        [
            [[1, date(2022, 10, 1)], [2, date(2022, 10, 2)], [3, date(2022, 10, 3)]],
            ["id", "test_col"],
            (date(2022, 10, 1), date(2022, 10, 2), date(2022, 10, 3)),
        ],
        [
            [
                [1, datetime(2022, 10, 1, 10, 0, 0)],
                [2, datetime(2022, 10, 1, 11, 0, 0)],
                [3, datetime(2022, 10, 1, 12, 0, 0)],
            ],
            ["id", "test_col"],
            (
                datetime(2022, 10, 1, 10, 0, 0),
                datetime(2022, 10, 1, 11, 0, 0),
                datetime(2022, 10, 1, 12, 0, 0),
            ),
        ],
    ],
    ids=(
        "tuple",
        "list",
        "value_int",
        "value_float",
        "data_float",
        "date",
        "timestamp",
    ),
)
def test_parameters(snowpark, data, columns, rule_value):
    df = snowpark.createDataFrame(data, columns)
    check = Check(CheckLevel.WARNING, "pytest")
    check.not_contained_in("TEST_COL", rule_value)
    rs = check.validate(df)
    assert rs.first().STATUS == "FAIL"


def test_coverage(snowpark):
    df = snowpark.createDataFrame(
        [[1, "blue"], [2, "green"], [3, "red"]], ["id", "desc"]
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.not_contained_in("DESC", ("blue", "red"), 0.2)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"
    assert rs.first().VIOLATIONS == 2
    assert rs.first().PASS_THRESHOLD == 0.2
    assert rs.first().PASS_RATE >= 0.2


def test_value_error():
    check = Check(CheckLevel.WARNING, "pytest")
    with pytest.raises(ValueError, match="Data types in rule values are inconsistent"):
        check.not_contained_in("VALUE", (10, "17"))
