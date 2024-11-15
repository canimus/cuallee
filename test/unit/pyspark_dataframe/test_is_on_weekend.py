import numpy as np
import pyspark.sql.functions as F

from cuallee.core.check import Check, CheckLevel


def test_positive(spark):
    df = spark.range(2).withColumn(
        "ARRIVAL_DATE", F.make_date(F.lit(2022), F.lit(11), F.col("id") + 12)
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_on_weekend("ARRIVAL_DATE")
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 0
    assert rs.first().pass_threshold == 1.0


def test_negative(spark):
    df = spark.range(5).withColumn(
        "ARRIVAL_DATE", F.make_date(F.lit(2022), F.lit(11), F.col("id") + 12)
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_on_weekend("ARRIVAL_DATE")
    rs = check.validate(df)
    assert rs.first().status == "FAIL"
    assert rs.first().violations == 3
    assert rs.first().pass_threshold == 1.0
    assert rs.first().pass_rate >= 2 / 5


def test_parameters(spark):
    return "😅 No parameters to be tested!"


def test_coverage(spark):
    df = spark.range(9).withColumn(
        "ARRIVAL_DATE", F.make_date(F.lit(2022), F.lit(11), F.col("id") + 12)
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_on_weekend("ARRIVAL_DATE", 0.4)
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 5
    assert rs.first().pass_threshold == 0.4
    assert np.allclose(rs.first().pass_rate, 4 / 9, rtol=0.001)
