from cuallee import Check, CheckLevel
import pandas as pd


def test_all_friday(spark):
    check = Check(CheckLevel.ERROR, "FridayTest")
    df = spark.createDataFrame(
        pd.date_range(start="2022-01-01", end="2022-01-10", freq="D")
        .rename("ts")
        .to_frame(),
        schema="ts timestamp",
    )
    check.is_on_friday("ts")

    assert (
        check.validate(spark, df).first().violations == 9
    ), "Incorrect calulation of Friday filters"
