from unittest import skip
from typing import Collection, Dict

import pytest
import operator
import snowflake.snowpark.functions as F
import snowflake.snowpark.types as T

from cuallee import Check, CheckLevel, CheckDataType
from cuallee import snow_validation as SV


def test_ComputeMethod():  # TODO:
    skip


def test_ComputeInstruction():  # TODO:
    skip


def test_Compute():  # TODO:
    skip


def test_field_type_filter(snowpark):
    df = snowpark.range(10)
    rs = SV._field_type_filter(df, T.IntegerType)
    assert (rs, Collection)


def test_numeric_fields(snowpark):
    df = (
        snowpark.range(10)
        .withColumn("desc", F.col("id").cast("string"))
        .withColumn("id2", F.col("id").cast("float"))
    )
    rs = SV.numeric_fields(df)
    assert (rs, Collection)
    assert len(rs) == 2
    assert rs == {
        "ID",
        "ID2",
    }  # Snowpark case sensitive --> everything is capitalised!!!


def test_string_fields(snowpark):
    df = snowpark.range(10).withColumn("desc", F.col("id").cast("string"))
    rs = SV.string_fields(df)
    assert (rs, Collection)
    assert len(rs) == 1
    assert rs == {"DESC"}


def test_date_fields(snowpark):
    df = (
        snowpark.range(10)
        .withColumn("date", F.date_from_parts(2022, 10, F.col("id")))
        .withColumn(
            "timestamp", F.timestamp_from_parts(2022, 10, F.col("id"), 10, 10, 10)
        )
        .withColumn("time", F.time_from_parts(F.col("id"), 5, 10))
    )
    rs = SV.date_fields(df)
    assert (rs, Collection)
    assert len(rs) == 2
    assert rs == {"DATE", "TIMESTAMP"}


def test_timestamp_fields(snowpark):
    df = (
        snowpark.range(10)
        .withColumn("date", F.date_from_parts(2022, 10, F.col("id")))
        .withColumn(
            "timestamp", F.timestamp_from_parts(2022, 10, F.col("id"), 10, 10, 10)
        )
        .withColumn("time", F.time_from_parts(F.col("id"), 5, 10))
    )
    rs = SV.timestamp_fields(df)
    assert (rs, Collection)
    assert len(rs) == 2
    assert rs == {"TIMESTAMP", "TIME"}


def test_column_set_comparison(snowpark):
    df = (
        snowpark.range(10)
        .withColumn("desc", F.col("id").cast("string"))
        .withColumn("id2", F.col("id").cast("float"))
    )
    check = (
        Check(CheckLevel.WARNING, "test_column_set_comparison")
        .is_complete("id")
        .is_complete("desc")
        .is_complete("id2")
    )
    rs = SV._column_set_comparison(
        check._rule,
        df,
        operator.attrgetter("column"),
        lambda x: x.data_type.name == CheckDataType.AGNOSTIC.name,
        SV.numeric_fields,
    )
    assert (rs, Collection)
    assert len(rs) == 1
    assert rs == {"DESC"}


def test_validate_numeric_type(snowpark):
    df = (
        snowpark.range(10)
        .withColumn("desc", F.col("id").cast("string"))
        .withColumn("id2", F.col("id").cast("float"))
    )
    check = (
        Check(CheckLevel.WARNING, "test_validate_numeric_type")
        .is_greater_than("id", 2)
        .is_greater_than("id2", 2)
    )
    rs = SV.validate_data_types(check._rule, df)
    assert (rs, bool)
    assert rs == True
    check.is_greater_than("desc", 2)
    with pytest.raises(AssertionError, match="are not numeric"):
        SV.validate_data_types(check._rule, df)


def test_validate_string_type(snowpark):
    df = snowpark.range(10).withColumn("desc", F.col("id").cast("string"))
    check = (
        Check(CheckLevel.WARNING, "test_validate_string_type")
        .has_pattern("id", "2")
        .has_pattern("desc", "2")
    )
    with pytest.raises(AssertionError, match="are not strings"):
        SV.validate_data_types(check._rule, df)


def test_validate_date_type(snowpark):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "test_validate_date_type").is_on_monday("id")
    with pytest.raises(AssertionError, match="are not dates"):
        SV.validate_data_types(check._rule, df)


def test_validate_timestamp_type(snowpark):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "test_validate_timestamp_type").is_on_schedule(
        "id", (F.time_from_parts(10, 0, 0), F.time_from_parts(17, 0, 0))
    )
    with pytest.raises(AssertionError, match="are not timestamps"):
        SV.validate_data_types(check._rule, df)


def test_get_compute_dictionary(snowpark):
    df = snowpark.range(10).withColumn("desc", F.col("id").cast("string"))
    check = (
        Check(CheckLevel.WARNING, "test_update_compute_dictionary")
        .is_complete("id")
        .is_complete("desc")
    )
    assert len(check._rule) == 2
    assert len(check._compute) == 0
    rs = SV.compute(check._rule)
    assert len(rs) == len(check._rule)
    assert set(rs.keys()) == set(check._rule.keys())


def test_compute_select_method(snowpark):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "test_compute_select_method").is_complete("id")
    rs = SV._compute_select_method(SV.compute(check._rule), df)
    assert (rs, Dict)
    assert len(rs) == 1


def test_compute_transform_method(): # TODO: when transform method available
    pass

def test_summary(): # TODO:
    pass