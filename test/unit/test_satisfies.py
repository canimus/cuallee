from cuallee import Check, CheckLevel
import pyspark.sql.functions as F
import pytest


def test_predicate_on_sql(spark):
    df = spark.range(10)
    check = Check(CheckLevel.ERROR, "SatisfiesTest")
    check.satisfies("((id BETWEEN 0 and 9) AND (id >= 0) AND (id <= 10))", "id")

    assert check.validate(spark, df).first().status == "PASS"


def test_predicate_on_multi_column(spark):
    df = spark.range(10).withColumn("id2", F.col("id") * 100)
    check = Check(CheckLevel.ERROR, "SatisfiesTest")
    check.satisfies("(id * id2) > 10", ["id", "id2"], 0.9)

    assert check.validate(spark, df).first().status == "PASS"


def test_unknown_columns(spark):
    df = spark.range(10).withColumn("id2", F.col("id") * 100)
    check = Check(CheckLevel.ERROR, "SatisfiesTest")
    check.satisfies("(id * id2) > 10", ["id", "id3"], 0.9)

    with pytest.raises(AssertionError, match=r".*id3.* not in dataframe"):
        check.validate(spark, df).first().status == "PASS"
