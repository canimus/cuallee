import pytest

from cuallee import Check, CheckLevel


def test_positive(spark):
    df = spark.createDataFrame(
        [[None], [None], [None], [None], [None]], schema="id int"
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_empty("id")
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 0
    assert rs.first().pass_threshold == 1.0


@pytest.mark.parametrize(
    "data, violation, pass_rate",
    [
        [[[0], [1], [None], [4], [5]], 4, 1 / 5],
        [[[0], [1], [None], [4], [None]], 3, 2 / 5],
    ],
    ids=("one_null_value", "two_null_value"),
)
def test_negative(spark, data, violation, pass_rate):
    df = spark.createDataFrame(data, schema="id int")
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_empty("id")
    rs = check.validate(df)
    assert rs.first().status == "FAIL"
    assert rs.first().violations == violation
    assert rs.first().pass_threshold == 1.0


def test_parameters():
    return "😅 No parameters to be tested!"


def test_coverage(spark):
    df = spark.createDataFrame([[0], [1], [None], [4], [5]], ["id"])
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_empty("id", 0.1)
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().pass_threshold == 0.1
