import pytest
import pyspark.sql.functions as F

from datetime import datetime, date
from cuallee import Check, CheckLevel


def test_positive(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_between("id", (0, 10))
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_negative(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_between("id", (0, 5))
    rs = check.validate(df)
    assert rs.first().status == "FAIL"


@pytest.mark.parametrize(
    "data, data_type, number, rule_column, rule_value",
    [
        ["id", "integer", 0, "id_2", tuple([0, 10])],
        ["id", "integer", 0, "id_2", list([0, 10])],
        ["id", "float", 0.0, "id_2", tuple([0, 10])],
        ["id", "integer", 1, "number", tuple([float(0.5), float(10.5)])],
    ],
    ids=[
        "tuple",
        "list",
        "data_as_float",
        "value_float",
    ],
)
def test_parameters(spark, data, data_type, number, rule_column, rule_value):
    df = spark.range(10).withColumn(rule_column, F.col(data).cast(data_type) + number)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_between(rule_column, rule_value)
    rs = check.validate(df)
    assert rs.first().status == "PASS"


@pytest.mark.parametrize(
    "col_days, col_hours, start_day, start_hour, data_type, rule_column, rule_value",
    [
        [
            "id",
            "zeros",
            1,
            0,
            "date",
            "date",
            tuple([date(2022, 9, 1), date(2022, 11, 1)]),
        ],
        [
            "zeros",
            "id",
            22,
            5,
            "timestamp",
            "timestamp",
            tuple([datetime(2022, 10, 22, 0, 0, 0), datetime(2022, 10, 22, 22, 0, 0)]),
        ],
    ],
    ids=["date", "timestamp"],
)
def test_parameters_dates(
    spark,
    col_days,
    col_hours,
    start_day,
    start_hour,
    data_type,
    rule_column,
    rule_value,
):
    df = (
        spark.range(10)
        .withColumn("zeros", F.lit(0))
        .withColumn(
            rule_column,
            F.concat(
                F.lit("2022-10-"),
                F.col(col_days) + start_day,
                F.lit(" "),
                F.col(col_hours) + start_hour,
                F.lit(":0:0"),
            ).cast(data_type),
        )
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_between(rule_column, rule_value)
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_coverage(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_between("id", (0, 5), 0.5)
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().pass_threshold == 0.5
    assert rs.first().pass_rate >= 6 / 10
