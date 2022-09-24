from pyspark.sql import SparkSession
from cuallee import CheckLevel, Check
import logging

logger = logging.getLogger(__name__)


def test_string_is_contained_in(spark: SparkSession):
    logger.info("TEST XXXX LOGGER@!!!!")
    df = spark.createDataFrame([[1, "blue"], [2, "green"], [3, "grey"]], ["ID", "desc"])
    c = Check(CheckLevel.WARNING, "is_contained_in_string_test")
    c.is_contained_in("desc", ("blue", "red"))
    rs = c.validate(spark, df)
    assert rs.select("status").collect()[0][0] == "FAIL"


def test_number_is_contained_in(spark: SparkSession):
    df = spark.createDataFrame([[1, 10], [2, 15], [3, 17]], ["ID", "value"])
    c = Check(CheckLevel.WARNING, "is_contained_in_number_test")
    c.is_contained_in("value", (10, 15, 20, 25))
    rs = c.validate(spark, df)
    assert rs.select("status").collect()[0][0] == "FAIL"
