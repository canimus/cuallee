from cuallee import Check, CheckLevel
import pyspark.sql.functions as F


def test_multiple_uniqueness(spark):
    df = spark.range(10).withColumn("id2", F.col("id") + 10)
    check = Check(CheckLevel.WARNING, "AreUniqueValues")
    check.are_unique(("id", "id2"))
    assert (
        check.validate(spark, df).first().status == "PASS"
    ), "Invalid duplicated record"
