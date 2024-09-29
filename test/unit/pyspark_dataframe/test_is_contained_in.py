from datetime import date, datetime

import numpy as np
import pytest

from cuallee import Check, CheckLevel


def test_positive(spark):
    df = spark.createDataFrame([[1, "blue"], [2, "green"], [3, "grey"]], ["id", "desc"])
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_contained_in("desc", ("blue", "red", "green", "grey", "black"))
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 0
    assert rs.first().pass_threshold == 1.0


def test_negative(spark):
    df = spark.createDataFrame([[1, "blue"], [2, "green"], [3, "grey"]], ["id", "desc"])
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_contained_in("desc", ("blue", "red"))
    rs = check.validate(df)
    assert rs.first().status == "FAIL"
    assert rs.first().violations == 2
    assert rs.first().pass_threshold == 1.0
    assert np.allclose(rs.first().pass_rate, 1 / 3, rtol=0.001)


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
def test_parameters(spark, data, columns, rule_value):
    df = spark.createDataFrame(data, columns)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_contained_in("test_col", rule_value)
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_coverage(spark):
    df = spark.createDataFrame([[1, "blue"], [2, "green"], [3, "red"]], ["id", "desc"])
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_contained_in("desc", ("blue", "red"), 0.5)
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 1
    assert rs.first().pass_threshold == 0.5
    assert np.allclose(rs.first().pass_rate, 2 / 3, rtol=0.001)


def test_value_error():
    check = Check(CheckLevel.WARNING, "pytest")
    with pytest.raises(ValueError, match="Data types in rule values are inconsistent"):
        check.is_contained_in("VALUE", (10, "17"))
