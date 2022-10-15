from pyspark.sql import SparkSession
from cuallee import Check, CheckLevel


def test_are_complete(spark: SparkSession):
    df = spark.createDataFrame([[1, 2], [None, 1]], ["A", "B"])
    c = Check(CheckLevel.WARNING, "are_complete_test")
    c.are_complete(("A", "B"))
    rs = c.validate(df)
    assert rs.select("pass_rate").collect()[0][0] == 0.75
    assert rs.select("status").collect()[0][0] == "FAIL"


def test_are_complete_col_list(spark: SparkSession):
    df = spark.createDataFrame([[1, 2], [None, 1]], ["A", "B"])
    c = Check(CheckLevel.WARNING, "are_complete_test")
    c.are_complete(["A", "B"])
    rs = c.validate(df)
    assert rs.select("pass_rate").collect()[0][0] == 0.75
    assert rs.select("status").collect()[0][0] == "FAIL"


def test_has_pattern(spark: SparkSession):
    df = spark.createDataFrame(
        [[1, "is_blue"], [2, "has_hat"], [3, "is_smart"]], ["ID", "desc"]
    )
    c = Check(CheckLevel.WARNING, "has_pattern_test")
    c.has_pattern("desc", "is")
    rs = c.validate(df)
    assert rs.select("status").collect()[0][0] == "FAIL"
