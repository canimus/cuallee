from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from cuallee import Check

def test_column_input(spark : SparkSession, check : Check):
    df = spark.range(10).withColumn("id2", F.col("id") * 10)
    check.are_complete("id", "id2")
    assert len(check._compute) == 1
    assert check._compute