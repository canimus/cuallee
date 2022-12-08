import pytest
import pyspark.sql.functions as F

from cuallee import Check, CheckLevel


def test_positive(spark):
    df = spark.range(10).withColumn("id2", F.col("id") + 10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.are_unique(("id", "id2"))
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_negative(spark):
    df = spark.createDataFrame(
        [[0, "zero"], [2, "deux"], [2, "deux"], [3, "trois"]], ["id", "desc"]
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.are_unique(("id", "desc"))
    rs = check.validate(df)
    assert rs.first().status == "FAIL"


@pytest.mark.parametrize(
    "rule_column", [tuple(["id", "id2"]), list(["id", "id2"])], ids=("tuple", "list")
)
def test_parameters(spark, rule_column):
    df = spark.range(10).withColumn("id2", F.col("id") + 10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.are_unique(rule_column)
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_coverage(spark):
    df = spark.createDataFrame(
        [[0, "zero"], [2, "deux"], [2, "deux"], [3, "trois"]], ["id", "desc"]
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.are_unique(("id", "desc"), 0.7)
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 1
    assert rs.first().pass_threshold == 0.7
    assert rs.first().pass_rate == 3 / 4
