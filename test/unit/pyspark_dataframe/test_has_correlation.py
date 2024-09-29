import numpy as np
import pandas as pd
import pyspark.sql.functions as F
import pytest

from cuallee import Check, CheckLevel


@pytest.mark.parametrize("type", ["integer", "double"], ids=["int", "float"])
def test_positive(spark, type):
    df = (
        spark.range(10)
        .select(F.col("id").cast(type))
        .withColumn("id2", F.col("id") * 10)
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_correlation("id", "id2", 1.0)
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().value == "1.0"


def test_negative(spark):
    df = spark.createDataFrame(
        pd.DataFrame({"id": np.arange(10), "id2": np.random.randn(10)})
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_correlation("id", "id2", 1.0)
    rs = check.validate(df)
    assert rs.first().status == "FAIL"


@pytest.mark.parametrize("rule_value", [int(1), float(1.0)], ids=["int", "float"])
def test_parameters(spark, rule_value):
    df = spark.range(10).withColumn("id2", F.col("id") * 10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_correlation("id", "id2", rule_value)
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_coverage():
    check = Check(CheckLevel.WARNING, "pytest")
    with pytest.raises(TypeError, match="positional arguments"):
        check.has_correlation("id", "id2", 1.0, 0.5)
