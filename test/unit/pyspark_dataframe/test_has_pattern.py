import pytest
import pyspark.sql.functions as F

from pyspark.sql import Row
from cuallee import Check, CheckLevel


def test_positive(spark):
    df = spark.createDataFrame(
        [
            Row(
                name,
            )
            for name in ["seis", "dos", "tres"]
        ],
        schema=["name"],
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_pattern("name", r".*s$")
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_negative(spark):
    df = spark.createDataFrame(
        [
            Row(
                name,
            )
            for name in ["uno", "dos", "tres"]
        ],
        schema=["name"],
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_pattern("name", r".*s$")
    rs = check.validate(df)
    assert rs.first().status == "FAIL"
    assert rs.first().violations == 1, "Uno should violate the expression"


def test_parameters():
    return "😅 No parameters to be tested!"


def test_legit(spark):
    df = spark.createDataFrame(
        [
            Row(
                name,
            )
            for name in ["seis", "dos", "tres"]
        ],
        schema=["name"],
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_legit("name")
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_not_legit(spark):
    df = spark.createDataFrame(
        [
            Row(
                name,
            )
            for name in ["seis", "dos", ""]
        ],
        schema=["name"],
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_legit("name")
    rs = check.validate(df)
    assert rs.first().status == "FAIL"


def test_coverage(spark):
    df = spark.createDataFrame(
        [
            Row(
                name,
            )
            for name in ["uno", "dos", "tres"]
        ],
        schema=["name"],
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_pattern("name", r".*s$", 0.65)
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().pass_threshold == 0.65
    assert rs.first().pass_rate == 2 / 3
