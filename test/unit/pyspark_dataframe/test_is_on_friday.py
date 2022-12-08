import pyspark.sql.functions as F

from cuallee import Check, CheckLevel


def test_positive(spark):
    df = spark.range(4).withColumn(
        "date", F.make_date(F.lit(2022), F.lit(12), 2 + F.col("id") * 7)
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_on_friday("date")
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 0
    assert rs.first().pass_threshold == 1.0


def test_negative(spark):
    df = spark.range(10).withColumn(
        "date", F.make_date(F.lit(2022), F.lit(12), 2 + F.col("id"))
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_on_friday("date")
    rs = check.validate(df)
    assert rs.first().status == "FAIL"
    assert rs.first().violations == 8
    assert rs.first().pass_threshold == 1.0
    assert rs.first().pass_rate == 0.2


def test_parameters(spark):
    return "😅 No parameters to be tested!"


def test_coverage(spark):
    df = spark.range(10).withColumn(
        "date", F.make_date(F.lit(2022), F.lit(12), 2 + F.col("id"))
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_on_friday("date", 0.2)
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 8
    assert rs.first().pass_threshold == 0.2
    assert rs.first().pass_rate == 0.2
