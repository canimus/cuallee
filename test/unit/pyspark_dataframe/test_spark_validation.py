import pytest
import pyspark.sql.functions as F

from pyspark.sql import DataFrame
from typing import Tuple, Dict
from toolz import valfilter  # type: ignore

from cuallee import Check, CheckLevel
from cuallee import pyspark_validation as SV
import importlib

minversion = pytest.mark.skipif(
    int(importlib.import_module("pyspark").__version__.replace(".", "")) < 330,
    reason="PySpark 3.3.0 Observation is required",
)


def test_compute_summary_return_dataframe(spark):
    df = spark.range(10).alias("id")
    c = Check(CheckLevel.WARNING, "test_spark_dataframe").is_complete("id")
    c._compute = SV.compute(c._rule)
    rs = SV.summary(c, df)
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

    rs = SV.compute(c._rule)
    assert len(rs) == len(c._rule)
    assert set(rs.keys()) == set(c._rule.keys())


# def test_overwrite_observe_method(spark):
#     c = (
#         Check(CheckLevel.WARNING, "test_overwrite_observe_method")
#         .is_complete("id")
#         .is_unique("id")
#         .is_greater_or_equal_than("id", 2)
#     )
#     c._compute = SV.compute(c._rule)
#     _observe = lambda x: x.compute_method == "observe"
#     _select = lambda x: x.compute_method == "select"
#     assert len(valfilter(_observe, c._compute)) == 2
#     assert len(valfilter(_select, c._compute)) == 1
#     SV._overwrite_observe_method(c)
#     assert len(valfilter(_observe, c._compute)) == 0
#     assert len(valfilter(_select, c._compute)) == 3


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
    with pytest.raises(AssertionError, match="not numeric"):
        SV.validate_data_types(c.rules, df)


def test_string_column_validation(spark):
    df = spark.range(10).alias("id").withColumn("desc", F.lit("red"))
    c = (
        Check(CheckLevel.WARNING, "test_col_not_string")
        .has_pattern("id", "2")
        .has_pattern("desc", "red")
    )
    with pytest.raises(AssertionError, match="not string"):
        SV.validate_data_types(c.rules, df)


def test_date_column_validation(spark):  # TODO: when got a case
    pass


def test_timestamp_column_validation(spark):  # TODO: when got a case
    pass


@minversion
def test_observe_method_return_tuple(spark):
    df = spark.range(10).alias("id")
    c = (
        Check(CheckLevel.WARNING, "test_observe_method")
        .is_complete("id")
        .is_unique("id")
    )
    row, observe = SV._compute_observe_method(SV.compute(c._rule), df)
    assert isinstance(row, int)
    assert isinstance(observe, Dict)
    assert row == df.count()
    assert len(observe) == 1


def test_observe_no_compute(spark):
    df = spark.range(10).alias("id")
    rs = (
        Check(CheckLevel.WARNING, "test_empty_observation").is_unique("id").validate(df)
    )
    assert isinstance(rs, DataFrame)


def test_select_method_return_dict(spark):
    df = spark.range(10).alias("id")
    c = (
        Check(CheckLevel.WARNING, "test_select_method")
        .is_complete("id")
        .is_unique("id")
    )
    select = SV._compute_select_method(SV.compute(c._rule), df)
    assert isinstance(select, Dict)
    assert len(select) == 1


def test_transform_method_return_dict(spark):
    df = spark.range(10).alias("id")
    c = (
        Check(CheckLevel.WARNING, "test_transform_method")
        .is_complete("id")
        .is_unique("id")
    )
    select = SV._compute_transform_method(SV.compute(c._rule), df)
    # assert isinstance(select, Dict)
    # assert len(select) == 1
    pass  # TODO: when method with transform


# def test_update_rule_status(spark):
#     df = spark.range(10).alias("id")
#     c = (
#         Check(CheckLevel.WARNING, "test_update_rule_status")
#         .is_complete("id")
#         .is_unique("id")
#     )
#     for v in c._rule.values():
#         assert v.status == None
#     df_summary = SV.summary(SV.compute(c._rule), df, spark)
#     for v in SV._get_rule_status(c, df_summary)._rule.values():
#         assert isinstance(v.status, str)
#         assert v.status == "PASS"


# def test_get_df_sample_return_dataframe(spark):
#     df = spark.range(10).alias("id")
#     c = (
#         Check(CheckLevel.WARNING, "test_update_rule_status")
#         .is_complete("id")
#         .is_unique("id")
#     )
#     c.validate(df)
#     rs = SV.get_record_sample(c, df, spark)
#     assert isinstance(rs, DataFrame)


# def test_get_sample_custom_status(spark):
#     df = spark.createDataFrame([[1, "1"], [2, None], [3, "3"]], ["id", "desc"])
#     c = (
#         Check(CheckLevel.WARNING, "test_update_rule_status")
#         .is_complete("id")
#         .is_complete("desc")
#         .is_greater_or_equal_than("id", 2)
#     )
#     c.validate(df)
#     rs_fail = SV.get_record_sample(c, df, spark)
#     rs_pass = SV.get_record_sample(c, df, spark, status="PASS")
#     rs_method = SV.get_record_sample(c, df, spark, method="is_complete")
#     assert rs_fail.count() == 2
#     assert rs_pass.count() == 1
#     assert rs_method.count() == 1


# ##__ TODO: __
# def test_get_spark_version():
#     pass


# def test_compute_class():
#     pass


# __End ToDo __
