from cuallee import Check, CheckLevel
import pandas as pd


def test_missing_one_weekday(spark):
    check = Check(CheckLevel.ERROR, "WeekdayMissingTest")
    df = spark.createDataFrame(
        pd.date_range(start="2022-01-01", end="2022-01-10", freq="D")
        .rename("ts")
        .to_frame()
        .reset_index(drop=True)
        .drop(4),
        schema="ts timestamp",
    )
    check.has_weekday_continuity("ts")

    assert (
        check.validate(spark, df).first().violations == 1
    ), "Incorrect calculation of continuity on business days"


def test_missing_two_weekdays(spark):
    check = Check(CheckLevel.ERROR, "WeekdayMissingTest")
    df = spark.createDataFrame(
        pd.date_range(start="2022-01-01", end="2022-01-10", freq="D")
        .rename("ts")
        .to_frame()
        .reset_index(drop=True)
        .drop(4)
        .drop(5),
        schema="ts timestamp",
    )
    check.has_weekday_continuity("ts")

    assert (
        check.validate(spark, df).first().violations == 2
    ), "Incorrect calculation of continuity on business days"
