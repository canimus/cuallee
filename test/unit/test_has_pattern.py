from cuallee import Check, CheckLevel
from pyspark.sql import DataFrame, Row
import pyspark.sql.functions as F

def test_regex_field(spark):
    df = spark.createDataFrame([Row(name,) for name in ["uno", "dos", "tres"]], schema="name string")
    check = Check(CheckLevel.ERROR, "RegExTest")
    check.has_pattern("name", r".*s$")
    assert check.validate(spark, df).first().violations == 1, "Uno should violate the expression"