from pyspark.sql import SparkSession
from cuallee import Check,  CheckLevel

def test_are_complete(spark: SparkSession):
    df = spark.createDataFrame([[1, 2], [None, 1]], ['A', 'B'])
    c = Check(CheckLevel.WARNING, 'are_complete_test')
    c.are_complete_1(('A', 'B'))
    rs = c.validate(spark, df)
    assert rs.select('obs_pct').collect()[0][0] == 0.75
    assert rs.select('status').collect()[0][0] == False

def test_are_complete_col_list(spark: SparkSession):
    df = spark.createDataFrame([[1, 2], [None, 1]], ['A', 'B'])
    c = Check(CheckLevel.WARNING, 'are_complete_test')
    c.are_complete_1(['A', 'B'])
    rs = c.validate(spark, df)
    assert rs.select('obs_pct').collect()[0][0] == 0.75
    assert rs.select('status').collect()[0][0] == False

def test_matches_regex(spark: SparkSession):
    df = spark.createDataFrame([[1, 'is_blue'], [2, 'has_hat'], [3, 'is_smart']], ['ID', 'desc'])
    c = Check(CheckLevel.WARNING, 'matches_regex_test')
    c.matches_regex('desc', 'is')
    rs = c.validate(spark, df)
    assert rs.select('status').collect()[0][0] == False

def test_is_contained_in(spark: SparkSession):
    df = spark.createDataFrame([[1, 10], [2, 15], [3, 17]], ['ID', 'values'])
    c = Check(CheckLevel.WARNING, 'is_contained_in_test')
    c.is_contained_in('values', (10,15,20,25))
    rs = c.validate(spark, df)
    assert rs.select('status').collect()[0][0] == False