import snowflake.snowpark.functions as F  # type: ignore

from datetime import date
from snowflake.snowpark import DataFrame  # type: ignore
from cuallee import Check, CheckLevel


def test_is_daily_high_violations(snowpark, configurations):
    df = snowpark.range(10).withColumn("date", F.date_from_parts(2022, F.col("id"), 1))
    check = Check(CheckLevel.WARNING, "check_is_daily_with_high_number_violation")
    check.is_daily("DATE")
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "FAIL"
    assert rs.first().VIOLATIONS == 189


def test_is_daily_low_violations(snowpark, configurations):
    df = (
        snowpark.range(10)
        .withColumn("date", F.date_from_parts(2022, 10, F.col("id") + 1))
        .withColumn(
            "date2",
            F.when(F.col("date") == date(2022, 10, 6), date(2022, 10, 11)).otherwise(
                F.col("date")
            ),
        )
    )
    check = Check(CheckLevel.WARNING, "check_is_daily_with_low_number_violation")
    check.is_daily("DATE2")
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "FAIL"
    assert rs.first().VIOLATIONS == 1


def test_is_daily_with_rows_eq_violations(snowpark, configurations):
    df = (
        snowpark.range(10)
        .withColumn("date", F.date_from_parts(2022, 10, F.col("id") + 1))
        .withColumn(
            "date2",
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
    check = Check(CheckLevel.WARNING, "check_is_daily_with_rows_eq_violations")
    check.is_daily("DATE2")
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "FAIL"
    assert rs.first().VIOLATIONS == 5
    assert rs.first().PASS_RATE == 0.5
