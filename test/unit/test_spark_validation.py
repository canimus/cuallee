from pyspark.sql import DataFrame
from cuallee import Check, CheckLevel
from cuallee.spark import spark_validation as SV


def test_compute_summary_return_dataframe(spark):
    df = spark.range(10).alias("id")
    c = Check(CheckLevel.WARNING, "test_spark_dataframe").is_complete("id")
    rs = SV.compute_summary(spark, df, c)
    assert isinstance(rs, DataFrame)


def test_observe_no_compute(spark):
    df = spark.range(10).alias("id")
    rs = (
        Check(CheckLevel.WARNING, "test_empty_observation")
        .is_unique("id")
        .validate(df, spark)
    )
    assert isinstance(rs, DataFrame)


##__ ToDo __

def test_computeInstruction():
    pass


def test_Compute():
    pass


def test_numeric_column_validation():
    pass


def test_string_column_validation():
    pass


def test_date_column_validation(spark):
    pass


def test_timestamp_column_validation(spark):
    pass

# __End ToDo __
