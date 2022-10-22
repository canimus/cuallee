from typing import Collection
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from cuallee import Check, ComputeInstruction
from operator import attrgetter as at
from toolz import compose


def test_integer_casting(spark: SparkSession, check: Check):
    df = spark.range(10)

    check.has_percentile("id", 10, 0.5)

    # Validate columns in ComputeInstruction
    assert "status" in check.validate(df).first().asDict()
