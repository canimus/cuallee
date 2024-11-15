import pyspark.sql.functions as F

from cuallee.core.check import Check, CheckLevel


def test_positive(spark):
    df = spark.range(5).withColumn(
        "ARRIVAL_DATE", F.make_date(F.lit(2022), F.lit(11), F.col("id") + 14)
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_on_weekday("ARRIVAL_DATE")
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 0
    assert rs.first().pass_threshold == 1.0


def test_negative(spark):
    df = spark.range(10).withColumn(
        "ARRIVAL_DATE", F.make_date(F.lit(2022), F.lit(11), F.col("id") + 14)
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_on_weekday("ARRIVAL_DATE")
    rs = check.validate(df)
    assert rs.first().status == "FAIL"
    assert rs.first().violations == 2
    assert rs.first().pass_threshold == 1.0
    assert rs.first().pass_rate >= 0.8


def test_parameters(spark):
    return "😅 No parameters to be tested!"


def test_coverage(spark):
    df = spark.range(10).withColumn(
        "ARRIVAL_DATE", F.make_date(F.lit(2022), F.lit(11), F.col("id") + 14)
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_on_weekday("ARRIVAL_DATE", 0.7)
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 2
    assert rs.first().pass_threshold == 0.7
    assert rs.first().pass_rate >= 0.8
