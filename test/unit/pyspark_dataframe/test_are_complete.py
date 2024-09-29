import pyspark.sql.functions as F
import pytest

from cuallee import Check, CheckLevel


def test_positive(spark):
    df = spark.range(10).withColumn("desc", F.col("id").cast("string"))
    check = Check(CheckLevel.WARNING, "pytest")
    check.are_complete(("id", "desc"))
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_negative(spark):
    df = spark.createDataFrame(
        [[0, "zero"], [1, None], [2, "deux"], [3, "trois"]], ["id", "desc"]
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.are_complete(("id", "desc"))
    rs = check.validate(df)
    assert rs.first().status == "FAIL"
    assert rs.first().violations == 1
    assert rs.first().pass_threshold == 1.0
    assert rs.first().pass_rate >= 3 / 4


@pytest.mark.parametrize(
    "rule_column", [tuple(["id", "desc"]), list(["id", "desc"])], ids=("tuple", "list")
)
def test_parameters(spark, rule_column):
    df = spark.range(10).withColumn("desc", F.col("id").cast("string"))
    check = Check(CheckLevel.WARNING, "pytest")
    check.are_complete(rule_column)
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_coverage(spark):
    df = spark.createDataFrame(
        [[0, "zero"], [1, None], [2, "deux"], [3, "trois"]], ["id", "desc"]
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.are_complete(("id", "desc"), 0.75)
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 1
    assert rs.first().pass_threshold == 0.75
    assert rs.first().pass_rate >= 3 / 4
