from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from cuallee import Check, CheckLevel
import inspect


def test_column_names_with_dots(spark: SparkSession, check: Check):
    df = spark.range(10).withColumn("id.2", F.col("id") * 10)
    col1 = "id"
    col2 = "id.2"
    check.are_complete((col1, col2))

    # Validate columns in ComputeInstruction
    assert eval(check.validate(df).select("column").first().column) == (
        col1,
        col2,
    ), "Invalid column names with dots"


def test_column_names_with_spaces(spark: SparkSession, check: Check):
    df = spark.range(10).withColumn("id 2", F.col("id") * 10)
    col1 = "id"
    col2 = "id 2"
    check.are_complete((col1, col2))

    # Validate columns in ComputeInstruction
    assert eval(check.validate(df).select("column").first().column) == (
        col1,
        col2,
    ), "Invalid column names with spaces"


def test_between_method():
    check = Check(CheckLevel.WARNING, "CheckIsBetween")
    assert callable(check.is_between)


def test_between_args():
    check = Check(CheckLevel.WARNING, "CheckIsBetween")
    signature = inspect.signature(check.is_between)
    params = signature.parameters
    # Expect column and array of values
    assert "column" in params.keys(), "Expected column parameter"
    assert "value" in params.keys(), "Expected value parameter"


def test_integer_casting(spark, check: Check):
    df = spark.range(10)

    check.has_percentile("id", 10, 0.5)

    # Validate columns in ComputeInstruction
    assert "status" in check.validate(df).first().asDict()
