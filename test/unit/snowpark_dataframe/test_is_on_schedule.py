import pytest
import snowflake.snowpark.functions as F  # type: ignore

from datetime import date
from cuallee import Check, CheckLevel


def test_positive(snowpark):
    df = snowpark.range(10).withColumn(
        "ARRIVAL_TIMESTAMP",
        F.timestamp_from_parts(2022, 1, F.col("id"), 10, F.col("id") * 10, 0),
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_on_schedule("ARRIVAL_TIMESTAMP", (9, 17))
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"
    assert rs.first().VIOLATIONS == 0
    assert rs.first().PASS_THRESHOLD == 1.0


def test_negative(snowpark):
    df = snowpark.range(10).withColumn(
        "ARRIVAL_TIMESTAMP",
        F.timestamp_from_parts(2022, 1, F.col("id"), 10, F.col("id") * 60, 0),
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_on_schedule("ARRIVAL_TIMESTAMP", (9, 17))
    rs = check.validate(df)
    assert rs.first().STATUS == "FAIL"
    assert rs.first().VIOLATIONS == 2
    assert rs.first().PASS_THRESHOLD == 1.0
    assert rs.first().PASS_RATE == 0.8


@pytest.mark.parametrize(
    "rule_value",
    [tuple([9, 17]), list([9, 17])],
    ids=(
        "tuple",
        "list",
    ),  # TODO: check for the init 'Any' but only integer can be passed!
)
def test_parameters(snowpark, rule_value):
    df = snowpark.range(10).withColumn(
        "ARRIVAL_TIMESTAMP",
        F.timestamp_from_parts(2022, 1, F.col("id"), 10, F.col("id") * 10, 0),
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_on_schedule("ARRIVAL_TIMESTAMP", rule_value)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"


def test_coverage(snowpark):
    df = snowpark.range(10).withColumn(
        "ARRIVAL_TIMESTAMP",
        F.timestamp_from_parts(2022, 1, F.col("id"), 10, F.col("id") * 60, 0),
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_on_schedule("ARRIVAL_TIMESTAMP", (9, 17), 0.8)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"
    assert rs.first().VIOLATIONS == 2
    assert rs.first().PASS_THRESHOLD == 0.8
    assert rs.first().PASS_RATE == 0.8
