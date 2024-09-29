from datetime import date

import pyspark.sql.functions as F
import pytest

from cuallee import Check, CheckLevel


def test_positive(spark):
    df = spark.range(10).withColumn(
        "date", F.make_date(F.lit(2022), F.lit(1), F.col("id") + 1)
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_daily("date")
    rs = check.validate(df)
    assert rs.first().status == "PASS"


@pytest.mark.parametrize(
    "col_months, start_month, col_days, column, violations",
    [
        ["id", 1, "zeros", "date", 188],
        ["zeros", 10, "id", "date2", 1],
    ],
    ids=("high_violations", "low_violations"),
)
def test_negative(spark, col_months, start_month, col_days, column, violations):
    df = (
        spark.range(10)
        .withColumn("zeros", F.lit(0))
        .withColumn(
            "date",
            F.make_date(
                F.lit(2022), F.col(col_months) + start_month, F.col(col_days) + 1
            ),
        )
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
    assert rs.first().status == "FAIL"
    assert rs.first().violations == violations


@pytest.mark.parametrize(
    "rule_value",
    [list([1, 2, 3, 4, 5]), list([1, 3, 5]), tuple([1, 3, 5])],
    ids=("default", "three_days_list", "three_days_tuple"),
)
def test_parameters(spark, rule_value):
    df = spark.range(31).withColumn(
        "date", F.make_date(F.lit(2022), F.lit(1), F.col("id") + 1)
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_daily("date")
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_coverage(spark):
    df = (
        spark.range(10)
        .withColumn("date", F.make_date(F.lit(2022), F.lit(10), F.col("id") + 1))
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
    check.is_daily("date_2", pct=0.5)
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 5
    assert rs.first().pass_threshold == 0.5
    assert rs.first().pass_rate >= 0.5
