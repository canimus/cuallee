import pytest
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from cuallee import Check, CheckLevel
from cuallee import dataframe as D
import logging

logger = logging.getLogger("test_validate")


def test_empty_dictionary(spark):
    df = spark.range(10).alias("id")
    with pytest.raises(
        Exception,
        match="Check is empty. Add validations i.e. is_complete, is_unique, etc.",
    ):
        Check(CheckLevel.WARNING, "test_empty_observation").validate(spark, df)


def test_pandas_dataframe(spark):
    df = pd.DataFrame({"id": [1, 2], "desc": ["1", "2"]})
    with pytest.raises(AssertionError) as e:
        Check(CheckLevel.WARNING, "test_empty_observation").is_complete("id").validate(
            spark, df
        )
        assert "Cualle operates only with Spark Dataframes" == str(e)


def test_column_name_validation(spark):
    df = spark.range(10).alias("id")
    with pytest.raises(Exception) as e:
        Check(CheckLevel.WARNING, "test_empty_observation").is_complete("ide").validate(
            spark, df
        )
        assert "Column(s): ide not in dataframe" == str(e)


def test_date_column_validation(spark):
    pass


def test_timestamp_column_validation(spark):
    pass


def test_observe_no_compute(spark):
    df = (
        spark.range(10)
        .alias("id")
        .withColumn("desc", F.lit(F.col("id").cast("string")))
    )
    rs = (
        Check(CheckLevel.WARNING, "test_empty_observation")
        .is_unique("id")
        .validate(spark, df)
    )
    assert isinstance(rs, DataFrame)
