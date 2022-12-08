import pytest
import pyspark.sql.functions as F

from datetime import date
from cuallee import Check, CheckLevel


def test_positive(spark):
    df = spark.range(10).withColumn("ARRIVAL_TIMESTAMP", F.to_timestamp(F.concat(F.lit("2022-01-"), F.col('id')+1, F.lit(" 10:"), (F.col('id')+1)*5, F.lit(":0"))))
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_on_schedule("ARRIVAL_TIMESTAMP", (9, 17))
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 0
    assert rs.first().pass_threshold == 1.0


def test_negative(spark):
    df = spark.range(10).withColumn("ARRIVAL_TIMESTAMP", F.to_timestamp(F.concat(F.lit("2022-01-"), F.col('id')+1, F.lit(" "), F.col('id')+10, F.lit(":0:0"))))
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_on_schedule("ARRIVAL_TIMESTAMP", (9, 17))
    rs = check.validate(df)
    assert rs.first().status == "FAIL"
    assert rs.first().violations == 2
    assert rs.first().pass_threshold == 1.0
    assert rs.first().pass_rate == 0.8


@pytest.mark.parametrize(
    "rule_value",
    [tuple([9, 17]), list([9, 17]), tuple([float(9.0), float(17.0)])],
    ids=(
        "tuple",
        "list",
        "float",
    ),
)
def test_parameters(spark, rule_value):
    df = spark.range(10).withColumn("ARRIVAL_TIMESTAMP", F.to_timestamp(F.concat(F.lit("2022-01-"), F.col('id')+1, F.lit(" 10:"), (F.col('id')+1)*5, F.lit(":0"))))
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_on_schedule("ARRIVAL_TIMESTAMP", rule_value)
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_coverage(spark):
    df = spark.range(10).withColumn("ARRIVAL_TIMESTAMP", F.to_timestamp(F.concat(F.lit("2022-01-"), F.col('id')+1, F.lit(" "), F.col('id')+10, F.lit(":0:0"))))
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_on_schedule("ARRIVAL_TIMESTAMP", (9, 17), 0.8)
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 2
    assert rs.first().pass_threshold == 0.8
    assert rs.first().pass_rate == 0.8
