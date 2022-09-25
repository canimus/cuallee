from typing import Collection
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from cuallee import Check, CheckLevel
from operator import attrgetter as at
from toolz import compose
import numpy as np
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def test_positive_correlation(spark: SparkSession):
    check = Check(CheckLevel.WARNING, "PyTestCheck")
    df = spark.range(10).withColumn("id2", F.col("id") * 10)
    check.has_correlation("id", "id2", 1.0)
    logger.info(check)
    logger.info(df.collect())
    test_result_row = check.validate(spark, df).first()
    logger.info(str(test_result_row))
    logger.info("STATUS")
    logger.info("*" * 100)
    logger.info(str(test_result_row.status))

    assert test_result_row.status == "PASS"


def test_no_correlation(spark: SparkSession):
    check = Check(CheckLevel.WARNING, "PyTestCheck")
    df = spark.createDataFrame(
        pd.DataFrame({"id": np.arange(10), "id2": np.random.randn(10)})
    )
    check.has_correlation("id", "id2", 1.0)
    assert check.validate(spark, df).first().status == "FAIL"


def test_correlation_value(spark: SparkSession):
    check = Check(CheckLevel.WARNING, "PyTestCheck")
    df = spark.range(10).withColumn("id2", F.col("id") * 10)
    check.has_correlation("id", "id2", 1.0)
    assert str(check.validate(spark, df).first().value) == "1.0"
