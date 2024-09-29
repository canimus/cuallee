import pandas as pd
import pytest
from pyspark.sql import DataFrame

from cuallee import Check, CheckLevel

# __ SPARK DATAFRAME TESTS __


@pytest.mark.skip
def test_return_spark_dataframe(spark):
    df = spark.range(10).alias("id")
    rs = Check(CheckLevel.WARNING, "test_spark_dataframe").is_complete("id")
    assert isinstance(rs.samples(rs.validate(df)), DataFrame)


@pytest.mark.skip
def test_order_validate_args(spark):
    df = spark.range(10).alias("id")
    with pytest.raises(
        AttributeError, match="'NoneType' object has no attribute 'show'"
    ):
        Check(CheckLevel.WARNING, "test_order_validate_args").is_complete(
            "id"
        ).sampling(spark, df).show()


@pytest.mark.skip
def test_spark_session_in_arg(spark):
    df = spark.range(10).alias("id")
    with pytest.raises(
        Exception,
        match="The function requires to pass a spark session as arg",
    ):
        Check(CheckLevel.WARNING, "test_spark_session_in_arg").is_complete(
            "id"
        ).sampling(df, "spark")
