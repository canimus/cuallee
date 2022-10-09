from cuallee import Check, CheckLevel
import pandas as pd


def test_all_weekends(spark):
    check = Check(CheckLevel.ERROR, "WeekendTest")
    df = spark.createDataFrame(
        pd.date_range(start="2022-01-01", end="2022-01-10", freq="D")
        .rename("ts")
        .to_frame(),
        schema="ts timestamp",
    )
    check.is_on_weekend("ts")

    assert (
        check.validate(spark, df).first().violations == 6
    ), "Incorrect calulation of Weekend filters"
