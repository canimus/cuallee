import pytest
import snowflake.snowpark.functions as F  # type: ignore

from datetime import date
from cuallee import Check, CheckLevel


def test_positive(snowpark):
    df = snowpark.range(10).withColumn(
        "DATE", F.date_from_parts(2022, 1, F.col("ID") + 1)
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_daily("DATE")
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"


@pytest.mark.parametrize(
    "data, column, violations",
    [
        [F.date_from_parts(2022, F.col("id"), 1), "DATE", 189],
        [F.date_from_parts(2022, 10, F.col("id") + 1), "DATE2", 1],
    ],
    ids=("high_violations", "low_violations"),
)
def test_negative(snowpark, data, column, violations):
    df = (
        snowpark.range(10)
        .withColumn("date", data)
        .withColumn(
            "date2",
            F.when(F.col("date") == date(2022, 10, 6), date(2022, 10, 11)).otherwise(
                F.col("date")
            ),
        )
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_daily(column)
    rs = check.validate(df)
    assert rs.first().STATUS == "FAIL"
    assert rs.first().VIOLATIONS == violations


@pytest.mark.parametrize(
    "rule_value",
    [list([1, 2, 3, 4, 5]), list([1, 3, 5]), tuple([1, 3, 5])],
    ids=("default", "three_days_list", "three_days_tuple"),
)
def test_parameters(snowpark, rule_value):
    df = snowpark.range(31).withColumn(
        "DATE", F.date_from_parts(2022, 1, F.col("ID") + 1)
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_daily("DATE")
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"


def test_coverage(snowpark):
    df = (
        snowpark.range(10)
        .withColumn("date", F.date_from_parts(2022, 10, F.col("id") + 1))
        .withColumn(
            "date_2",
            F.when(
                F.col("date").isin(
                    [
                        date(2022, 10, 3),
                        date(2022, 10, 4),
                        date(2022, 10, 5),
                        date(2022, 10, 6),
                        date(2022, 10, 7),
                    ]
                ),
                F.col("date") + 7,
            ).otherwise(F.col("date")),
        )
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_daily("DATE_2", pct=0.5)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"
    assert rs.first().VIOLATIONS == 5
    assert rs.first().PASS_THRESHOLD == 0.5
    assert rs.first().PASS_RATE == 0.5
