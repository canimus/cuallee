from typing import Collection
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from cuallee import Check, ComputeInstruction
from operator import attrgetter as at
from toolz import compose

def test_column_input_list(spark : SparkSession, check : Check):
    df = spark.range(10).withColumn("id2", F.col("id") * 10)
    col1 = "id"
    col2 = "id2"
    check.are_complete([col1, col2])
    
    # Validate columns in ComputeInstruction
    assert not isinstance(list(check._compute.values())[0].rule.column, str), "Expected list of columns"
    
def test_column_input_tuple(spark : SparkSession, check : Check):
    df = spark.range(10).withColumn("id2", F.col("id") * 10)
    col1 = "id"
    col2 = "id2"
    check.are_complete((col1, col2))
    
    # Validate columns in ComputeInstruction
    assert not isinstance(list(check._compute.values())[0].rule.column, str), "Expected tuple of columns"

def test_column_names_with_dots(spark : SparkSession, check : Check):
    df = spark.range(10).withColumn("id.2", F.col("id") * 10)
    col1 = "id"
    col2 = "id.2"
    check.are_complete((col1, col2))
    
    # Validate columns in ComputeInstruction
    assert eval(check.validate(spark, df).select("column").first().column) == (col1, col2), "Invalid column names with dots"

def test_column_names_with_spaces(spark : SparkSession, check : Check):
    df = spark.range(10).withColumn("id 2", F.col("id") * 10)
    col1 = "id"
    col2 = "id 2"
    check.are_complete((col1, col2))
    
    # Validate columns in ComputeInstruction
    assert eval(check.validate(spark, df).select("column").first().column) == (col1, col2), "Invalid column names with spaces"