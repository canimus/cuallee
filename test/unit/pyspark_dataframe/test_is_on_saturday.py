from cuallee import Check, CheckLevel
import pandas as pd


def test_all_saturday(spark):
    check = Check(CheckLevel.ERROR, "SaturdayTest")
    df = spark.createDataFrame(
        pd.date_range(start="2022-01-01", end="2022-01-10", freq="D")
        .rename("ts")
        .to_frame(),
        schema="ts timestamp",
    )
    check.is_on_saturday("ts")

    assert (
        check.validate(df).first().violations == 8
    ), "Incorrect calulation of Saturday filters"
