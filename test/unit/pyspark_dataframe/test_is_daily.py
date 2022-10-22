from cuallee import Check, CheckLevel
import pandas as pd
import pyspark.sql.functions as F


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
    check.is_daily("ts")

    assert (
        check.validate(df.withColumn("ts", F.to_date("ts"))).first().violations == 1
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
    check.is_daily("ts")

    assert (
        check.validate(df.withColumn("ts", F.to_date("ts"))).first().violations == 2
    ), "Incorrect calculation of continuity on business days"


def test_missing_sunday(spark):
    check = Check(CheckLevel.ERROR, "WeekdayMissingTest")
    df = spark.createDataFrame(
        pd.date_range(start="2022-01-01", end="2022-01-18", freq="D")
        .rename("ts")
        .to_frame()
        .reset_index(drop=True)
        .drop(1)
        .drop(8),
        schema="ts timestamp",
    )
    # Delivery on Sundays only, has continuity
    check.is_daily("ts", [1])

    assert (
        check.validate(df.withColumn("ts", F.to_date("ts"))).first().violations == 2
    ), "Incorrect calculation of continuity on business days"


def test_missing_mon_wed_fri(spark):
    check = Check(CheckLevel.ERROR, "MonWedFriTest")
    df = spark.createDataFrame(
        pd.date_range(start="2022-01-01", end="2022-01-31", freq="D")
        .rename("ts")
        .to_frame()
        .reset_index(drop=True)
        .drop(9)
        .drop(11)
        .drop(13),
        schema="ts timestamp",
    )
    # Delivery on Mon-Wed-Fri only, has continuity
    check.is_daily("ts", [2, 4, 6])

    assert (
        check.validate(df.withColumn("ts", F.to_date("ts"))).first().violations == 3
    ), "Incorrect calculation of continuity on business days"
