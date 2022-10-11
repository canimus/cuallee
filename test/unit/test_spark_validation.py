import pytest
import pyspark.sql.functions as F

from pyspark.sql import DataFrame
from typing import Tuple, Dict
from toolz import valfilter  # type: ignore

from cuallee import Check, CheckLevel
from cuallee.spark import spark_validation as SV


def test_compute_summary_return_dataframe(spark):
    df = spark.range(10).alias("id")
    c = Check(CheckLevel.WARNING, "test_spark_dataframe").is_complete("id")
    SV._get_compute_dict(c)
    rs = SV.compute_summary(c, df, spark)
    assert isinstance(rs, DataFrame)


def test_get_compute_dictionary(spark):
    df = spark.range(10).alias("id")
    c = (
        Check(CheckLevel.WARNING, "test_update_compute_dictionary")
        .is_complete("id")
        .is_unique("id")
        .is_greater_or_equal_than("id", 2)
    )
    assert len(c._rule) == 3
    assert len(c._compute) == 0
    rs = SV._get_compute_dict(c)
    assert len(c._compute) == len(c._rule)
    assert set(c._compute.keys()) == set(c._rule.keys())


def test_overwrite_observe_method(spark):
    c = (
        Check(CheckLevel.WARNING, "test_overwrite_observe_method")
        .is_complete("id")
        .is_unique("id")
        .is_greater_or_equal_than("id", 2)
    )
    SV._get_compute_dict(c)
    _observe = lambda x: x.compute_method == 'observe'
    _select = lambda x: x.compute_method == 'select'
    assert len(valfilter(_observe, c._compute)) == 2
    assert len(valfilter(_select, c._compute)) == 1
    SV._overwrite_observe_method(c)
    assert len(valfilter(_observe, c._compute)) == 0
    assert len(valfilter(_select, c._compute)) == 3


def test_numeric_column_validation(spark):
    df = (
        spark.range(10)
        .alias("id")
        .withColumn("desc", F.lit(F.col("id").cast("string")))
    )
    c = (
        Check(CheckLevel.WARNING, "test_col_not_numeric")
        .is_greater_or_equal_than("id", 2)
        .is_greater_or_equal_than("desc", 2)
    )
    with pytest.raises(AssertionError, match="are not numeric"):
        SV._validate_dataTypes(c, df)


def test_string_column_validation(spark):
    df = spark.range(10).alias("id").withColumn("desc", F.lit("red"))
    c = (
        Check(CheckLevel.WARNING, "test_col_not_string")
        .matches_regex("id", "2")
        .matches_regex("desc", "red")
    )
    with pytest.raises(AssertionError, match="are not strings"):
        SV._validate_dataTypes(c, df)


def test_date_column_validation(spark):  # ToDo when got a case
    pass


def test_timestamp_column_validation(spark):  # ToDo when got a case
    pass


def test_observe_method_return_tuple(spark):
    df = spark.range(10).alias("id")
    c = (
        Check(CheckLevel.WARNING, "test_observe_method")
        .is_complete("id")
        .is_unique("id")
    )
    row, observe = SV._compute_observe_method(SV._get_compute_dict(c), df)
    assert isinstance(row, int)
    assert isinstance(observe, Dict)
    assert row == df.count()
    assert len(observe) == 1


def test_observe_no_compute(spark):
    df = spark.range(10).alias("id")
    rs = (
        Check(CheckLevel.WARNING, "test_empty_observation")
        .is_unique("id")
        .validate(df, spark)
    )
    assert isinstance(rs, DataFrame)


def test_select_method_return_dict(spark):
    df = spark.range(10).alias("id")
    c = (
        Check(CheckLevel.WARNING, "test_select_method")
        .is_complete("id")
        .is_unique("id")
    )
    select = SV._compute_select_method(SV._get_compute_dict(c), df)
    assert isinstance(select, Dict)
    assert len(select) == 1


def test_transform_method_return_dict(spark):
    df = spark.range(10).alias("id")
    c = (
        Check(CheckLevel.WARNING, "test_transform_method")
        .is_complete("id")
        .is_unique("id")
    )
    select = SV._compute_transform_method(SV._get_compute_dict(c), df)
    # assert isinstance(select, Dict)
    # assert len(select) == 1
    pass  # To Do when method with transform


def test_update_rule_status(spark):
    df = spark.range(10).alias("id")
    c = (
        Check(CheckLevel.WARNING, "test_update_rule_status")
        .is_complete("id")
        .is_unique("id")
    )
    for v in c._rule.values():
        assert v.status == None
    df_summary = SV.compute_summary(SV._get_compute_dict(c), df, spark)
    for v in SV._get_rule_status(c, df_summary)._rule.values():
        assert isinstance(v.status, str)
        assert v.status == "PASS"


def test_get_df_sample_return_dataframe(spark):
    df = spark.range(10).alias("id")
    c = (
        Check(CheckLevel.WARNING, "test_update_rule_status")
        .is_complete("id")
        .is_unique("id")
    )
    c.validate(df, spark)
    rs = SV.get_record_sample(c, df, spark)
    assert isinstance(rs, DataFrame)


def test_get_sample_custom_status(spark):
    df = spark.createDataFrame([[1, "1"], [2, None], [3, "3"]], ["id", "desc"])
    c = (
        Check(CheckLevel.WARNING, "test_update_rule_status")
        .is_complete("id")
        .is_complete("desc")
        .is_greater_or_equal_than("id", 2)
    )
    c.validate(df, spark)
    rs_fail = SV.get_record_sample(c, df, spark)
    rs_pass = SV.get_record_sample(c, df, spark, status="PASS")
    rs_method = SV.get_record_sample(c, df, spark, method="is_complete")
    assert rs_fail.count() == 2
    assert rs_pass.count() == 1
    assert rs_method.count() == 1


##__ ToDo __
def test_get_spark_version():
    pass

def test_compute_class():
    pass


# __End ToDo __
