import pytest
from unittest.mock import patch
import pyspark.sql.functions as F
import pyspark.sql.types as T

from pyspark.sql import DataFrame, SparkSession
from typing import Tuple, Dict, Set, List
from toolz import valfilter  # type: ignore

from cuallee import Check, CheckLevel
from cuallee import pyspark_validation as PSV
import importlib


def test_compute_method():
    assert callable(PSV.ComputeMethod)
    assert hasattr(PSV.ComputeMethod, "SELECT")
    assert hasattr(PSV.ComputeMethod, "TRANSFORM")
    assert hasattr(PSV.ComputeMethod, "OBSERVE")


def test_compute_instruction():
    assert callable(PSV.ComputeInstruction)
    instruction = PSV.ComputeInstruction("a", "b", "c")
    assert hasattr(instruction, "predicate")
    assert hasattr(instruction, "expression")
    assert hasattr(instruction, "compute_method")
    assert "ComputeInstruction" in str(instruction)


def test_compute():
    assert callable(PSV.Compute)
    compute = PSV.Compute()
    assert hasattr(compute, "compute_instruction")
    assert hasattr(compute, "_sum_predicate_to_integer")
    assert hasattr(compute, "_single_value_rule")
    assert hasattr(compute, "_stats_fn_rule")
    assert hasattr(compute, "is_complete")
    assert hasattr(compute, "are_complete")
    assert hasattr(compute, "is_unique")
    assert hasattr(compute, "are_unique")
    assert hasattr(compute, "is_greater_than")
    assert hasattr(compute, "is_greater_or_equal_than")
    assert hasattr(compute, "is_less_than")
    assert hasattr(compute, "is_less_or_equal_than")
    assert hasattr(compute, "is_equal_than")
    assert hasattr(compute, "has_pattern")
    assert hasattr(compute, "has_min")
    assert hasattr(compute, "has_max")
    assert hasattr(compute, "has_mean")
    assert hasattr(compute, "has_std")
    assert hasattr(compute, "has_sum")
    assert hasattr(compute, "is_between")
    assert hasattr(compute, "is_contained_in")
    assert hasattr(compute, "has_percentile")
    assert hasattr(compute, "is_inside_interquartile_range")
    assert hasattr(compute, "has_min_by")
    assert hasattr(compute, "has_max_by")
    assert hasattr(compute, "has_correlation")
    assert hasattr(compute, "satisfies")
    assert hasattr(compute, "has_entropy")
    assert hasattr(compute, "is_on_weekday")
    assert hasattr(compute, "is_on_weekend")
    assert hasattr(compute, "is_on_monday")
    assert hasattr(compute, "is_on_tuesday")
    assert hasattr(compute, "is_on_wednesday")
    assert hasattr(compute, "is_on_thursday")
    assert hasattr(compute, "is_on_friday")
    assert hasattr(compute, "is_on_saturday")
    assert hasattr(compute, "is_on_sunday")
    assert hasattr(compute, "is_on_schedule")
    assert hasattr(compute, "is_daily")
    assert hasattr(compute, "has_workflow")


def test_field_type_filter(spark):
    df = spark.range(10)
    rs = PSV._field_type_filter(df, T.IntegerType)
    assert isinstance(rs, Set)


def test_numeric_fields(spark):
    df = (
        spark.range(10)
        .withColumn("desc", F.col("id").cast("string"))
        .withColumn("id2", F.col("id").cast("float"))
    )
    rs = PSV.numeric_fields(df)
    assert isinstance(rs, Set)
    assert len(rs) == 2
    assert rs == {"id", "id2"}


def test_string_fields(spark):
    df = spark.range(10).withColumn("desc", F.col("id").cast("string"))
    rs = PSV.string_fields(df)
    assert isinstance(rs, Set)
    assert len(rs) == 1
    assert rs == {"desc"}


def test_date_fields(spark):
    df = (
        spark.range(10)
        .withColumn("date", F.make_date(F.lit(2022), F.lit(10), F.col("id") + 1))
        .withColumn(
            "timestamp",
            F.to_timestamp(
                F.concat(
                    F.lit("2022-10-"),
                    (F.col("id") + 1).cast("string"),
                    F.lit(" 10:10:10"),
                )
            ),
        )
    )
    rs = PSV.date_fields(df)
    assert isinstance(rs, Set)
    assert len(rs) == 2
    assert rs == {"date", "timestamp"}


def test_timestamp_fields(spark):
    df = (
        spark.range(10)
        .withColumn("date", F.make_date(F.lit(2022), F.lit(10), F.col("id") + 1))
        .withColumn(
            "timestamp",
            F.to_timestamp(
                F.concat(
                    F.lit("2022-10-"),
                    (F.col("id") + 1).cast("integer"),
                    F.lit(" 10:10:10"),
                )
            ),
        )
    )
    rs = PSV.timestamp_fields(df)
    assert isinstance(rs, Set)
    assert len(rs) == 1
    assert rs == {"timestamp"}


def test_validate_columns(spark):
    df = spark.range(10).withColumn("id2", F.col("id").cast("float"))
    check = (
        Check(CheckLevel.WARNING, "test_validate_column_name")
        .is_complete("ID")
        .is_greater_than("id2", 2)
    )
    rs = PSV.validate_data_types(check.rules, df)
    assert isinstance(rs, bool)
    assert rs == True
    check.is_complete("id3")
    with pytest.raises(AssertionError, match="are not present in dataframe"):
        PSV.validate_data_types(check.rules, df)


def test_numeric_column_validation(spark):
    df = (
        spark.range(10)
        .withColumn("desc", F.lit(F.col("id").cast("string")))
        .withColumn("id2", F.col("id").cast("float"))
    )
    check = (
        Check(CheckLevel.WARNING, "test_col_not_numeric")
        .is_greater_or_equal_than("id", 2)
        .is_greater_or_equal_than("id2", 2)
    )
    rs = PSV.validate_data_types(check.rules, df)
    assert isinstance(rs, bool)
    assert rs == True
    check.is_greater_than("desc", 2)
    with pytest.raises(AssertionError, match="are not numeric"):
        PSV.validate_data_types(check.rules, df)


def test_string_column_validation(spark):
    df = spark.range(10).withColumn("desc", F.col("id").cast("string"))
    check = (
        Check(CheckLevel.WARNING, "test_validate_string_type")
        .has_pattern("id", "2")
        .has_pattern("desc", "2")
    )
    with pytest.raises(AssertionError, match="are not string"):
        PSV.validate_data_types(check.rules, df)


def test_date_column_validation(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "test_validate_date_type").is_on_monday("id")
    with pytest.raises(AssertionError, match="are not date"):
        PSV.validate_data_types(check.rules, df)


def test_timestamp_column_validation(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "test_validate_timestamp_type").is_on_schedule(
        "id", (10, 17)
    )
    with pytest.raises(AssertionError, match="are not timestamp"):
        PSV.validate_data_types(check.rules, df)


def test_get_compute_dictionary(spark):
    df = spark.range(10)
    check = (
        Check(CheckLevel.WARNING, "test_update_compute_dictionary")
        .is_complete("id")
        .is_unique("id")
        .is_greater_or_equal_than("id", 2)
    )
    assert len(check._rule) == 3

    rs = PSV.compute(check._rule)
    assert len(rs) == len(check._rule)
    assert set(rs.keys()) == set(check._rule.keys())


def test_overwrite_observe_method(spark):
    check = (
        Check(CheckLevel.WARNING, "test_overwrite_observe_method")
        .is_complete("id")
        .is_unique("id")
        .is_greater_or_equal_than("id", 2)
    )
    computed_expression_dict = PSV.compute(check._rule)
    _observe = lambda x: x.compute_method.name == PSV.ComputeMethod.OBSERVE.name
    _select = lambda x: x.compute_method.name == PSV.ComputeMethod.SELECT.name
    assert len(valfilter(_observe, computed_expression_dict)) == 2
    assert len(valfilter(_select, computed_expression_dict)) == 1
    new_computed_expression_dict = PSV._replace_observe_compute(
        computed_expression_dict
    )
    assert len(valfilter(_observe, new_computed_expression_dict)) == 0
    assert len(valfilter(_select, new_computed_expression_dict)) == 3


minversion = pytest.mark.skipif(
    int(importlib.import_module("pyspark").__version__.replace(".", "")) < 330,
    reason="PySpark 3.3.0 Observation is required",
)


@minversion
def test_observe_method_return_tuple(spark):
    df = spark.range(10)
    check = (
        Check(CheckLevel.WARNING, "test_observe_method")
        .is_complete("id")
        .is_unique("id")
    )
    row, observe = PSV._compute_observe_method(PSV.compute(check._rule), df)
    assert isinstance(row, int)
    assert isinstance(observe, Dict)
    assert row == df.count()
    assert len(observe) == 1


def test_observe_no_compute(spark):
    df = spark.range(10)
    rs = (
        Check(CheckLevel.WARNING, "test_empty_observation").is_unique("id").validate(df)
    )
    assert isinstance(rs, DataFrame)


def test_compute_select_method(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "test_select_method").is_unique("id")
    select = PSV._compute_select_method(PSV.compute(check._rule), df)
    assert isinstance(select, Dict)
    assert len(select) == 1


def test_compute_transform_method(spark):
    df = spark.range(10).withColumn(
        "date", F.make_date(F.lit(2022), F.col("id") + 1, F.lit(1))
    )
    check = Check(CheckLevel.WARNING, "test_transform_method").is_daily("date")
    transform = PSV._compute_transform_method(PSV.compute(check._rule), df)
    assert isinstance(transform, Dict)
    assert len(transform) == 1


def test_compute_summary_return_dataframe(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "test_spark_dataframe").is_complete("id")
    rs = PSV.summary(check, df)
    assert isinstance(rs, DataFrame)


@patch.object(SparkSession, "version", "3.2.0")
def test_lower_spark_version(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "test_spark_dataframe").is_complete("id")
    rs = PSV.summary(check, df)
    assert isinstance(rs, DataFrame)
    assert str(spark.version) == "3.2.0"


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
