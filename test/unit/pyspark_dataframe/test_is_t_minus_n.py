import pyspark.sql.functions as F

from cuallee import Check, CheckLevel


def test_positive(spark):
    df = spark.range(10).withColumn("value", F.date_add(F.current_date(), 0))
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_t_minus_n("value", 0)
    check.is_today("value")
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 0
    assert rs.first().pass_threshold == 1.0


def test_negative(spark):
    df = spark.range(10).withColumn("value", F.date_add(F.current_date(), 1))
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_t_minus_n("value", 0)
    rs = check.validate(df)
    assert rs.first().status == "FAIL"
    assert rs.first().violations == 10
    assert rs.first().pass_threshold == 1.0
    assert rs.first().pass_rate == 0.0


def test_parameters(spark):
    return "😅 No parameters to be tested!"


def test_coverage(spark):
    df = spark.range(10).withColumn("value", F.date_add(F.current_date(), -1))
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_t_minus_n("value", 0, 0.9)
    check.is_yesterday("value")
    rs = check.validate(df)
    assert rs.first().status == "FAIL"
    assert rs.first().violations == 10.0
    assert rs.first().pass_threshold == 0.9
    assert rs.first().pass_rate == 0.0
