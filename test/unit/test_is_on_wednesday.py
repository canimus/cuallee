from cuallee import Check, CheckLevel
import pandas as pd


def test_all_wednesday(spark):
    check = Check(CheckLevel.ERROR, "WednesdayTest")
    df = spark.createDataFrame(
        pd.date_range(start="2022-01-01", end="2022-01-10", freq="D")
        .rename("ts")
        .to_frame(),
        schema="ts timestamp",
    )
    check.is_on_wednesday("ts")

    assert (
        check.validate(df).first().violations == 9
    ), "Incorrect calulation of Wednesday filters"
