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