from cuallee import Check, CheckLevel
import pandas as pd


def test_all_weekdays(spark):
    check = Check(CheckLevel.ERROR, "WeekdayTest")
    df = spark.createDataFrame(
        pd.date_range(start="2022-01-01", end="2022-01-10", freq="D")
        .rename("ts")
        .to_frame(),
        schema="ts timestamp",
    )
    check.is_on_weekday("ts")

    assert (
        check.validate(df).first().violations == 4
    ), "Incorrect calulation of Weekday filters"
