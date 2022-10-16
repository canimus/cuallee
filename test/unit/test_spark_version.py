from unittest.mock import patch
from pyspark.sql.session import SparkSession


@patch.object(SparkSession, "version", "3.0.0")
def test_older_version(spark):
    df = spark.range(10)
    assert df.count() == 10
    assert str(spark.version) == "3.0.0"