from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from cuallee import Check


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
