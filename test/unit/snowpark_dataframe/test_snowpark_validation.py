from unittest import skip
from typing import Collection, Dict
from snowflake.snowpark import DataFrame  # type: ignore

import pytest
import operator
import snowflake.snowpark.functions as F  # type: ignore
import snowflake.snowpark.types as T  # type: ignore

from cuallee import Check, CheckLevel, CheckDataType
from cuallee import snowpark_validation as SV


def test_compute_method():  # TODO:
    skip


def test_compute_instruction():  # TODO:
    skip


def test_compute():  # TODO:
    skip


def test_field_type_filter(snowpark):
    df = snowpark.range(10)
    rs = SV._field_type_filter(df, T.IntegerType)
    assert isinstance(rs, Collection)


def test_numeric_fields(snowpark):
    df = (
        snowpark.range(10)
        .withColumn("desc", F.col("id").cast("string"))
        .withColumn("id2", F.col("id").cast("float"))
    )
    rs = SV.numeric_fields(df)
    assert isinstance(rs, Collection)
    assert len(rs) == 2
    assert rs == {
        "ID",
        "ID2",
    }  # Snowpark case sensitive --> everything is capitalised!!!


def test_string_fields(snowpark):
    df = snowpark.range(10).withColumn("desc", F.col("id").cast("string"))
    rs = SV.string_fields(df)
    assert isinstance(rs, Collection)
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
    assert isinstance(rs, Collection)
    assert len(rs) == 2
    assert rs == {"DATE", "TIMESTAMP"}


def test_timestamp_fields(snowpark):
    df = (
        snowpark.range(10)
        .withColumn("date", F.date_from_parts(2022, 10, F.col("id")))
        .withColumn(
            "timestamp", F.timestamp_from_parts(2022, 10, F.col("id"), 10, 10, 10)
        )
    )
    rs = SV.timestamp_fields(df)
    assert isinstance(rs, Collection)
    assert len(rs) == 1
    assert rs == {"TIMESTAMP"}


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
    assert isinstance(rs, Collection)
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
        .is_greater_than("ID", 2)
        .is_greater_than("ID2", 2)
    )
    rs = SV.validate_data_types(check.rules, df)
    assert isinstance(rs, bool)
    assert rs == True
    check.is_greater_than("DESC", 2)
    with pytest.raises(AssertionError, match="are not numeric"):
        SV.validate_data_types(check.rules, df)


def test_validate_string_type(snowpark):
    df = snowpark.range(10).withColumn("desc", F.col("id").cast("string"))
    check = (
        Check(CheckLevel.WARNING, "test_validate_string_type")
        .has_pattern("ID", "2")
        .has_pattern("DESC", "2")
    )
    with pytest.raises(AssertionError, match="are not string"):
        SV.validate_data_types(check.rules, df)


def test_validate_date_type(snowpark):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "test_validate_date_type").is_on_monday("ID")
    with pytest.raises(AssertionError, match="are not date"):
        SV.validate_data_types(check.rules, df)


def test_validate_timestamp_type(snowpark):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "test_validate_timestamp_type").is_on_schedule(
        "ID", (F.time_from_parts(10, 0, 0), F.time_from_parts(17, 0, 0))
    )
    with pytest.raises(AssertionError, match="are not timestamp"):
        SV.validate_data_types(check.rules, df)


def test_get_compute_dictionary(snowpark):
    df = snowpark.range(10).withColumn("desc", F.col("id").cast("string"))
    check = (
        Check(CheckLevel.WARNING, "test_update_compute_dictionary")
        .is_complete("id")
        .is_complete("desc")
    )
    assert len(check._rule) == 2
    
    rs = SV.compute(check._rule)
    assert len(rs) == len(check._rule)
    assert set(rs.keys()) == set(check._rule.keys())


def test_compute_select_method(snowpark):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "test_compute_select_method").is_complete("id")
    rs = SV._compute_select_method(SV.compute(check._rule), df)
    assert isinstance(rs, Dict)
    assert len(rs) == 1


def test_compute_transform_method(snowpark):
    df = snowpark.range(10).withColumn("date", F.date_from_parts(2022, F.col("id"), 1))
    check = Check(CheckLevel.WARNING, "check_compute_transform_method")
    check.is_daily("DATE")
    rs = SV._compute_transform_method(SV.compute(check._rule), df)
    assert isinstance(rs, Dict)
    assert len(rs) == 1


def test_summary(snowpark, configurations):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "test_compute_select_method").is_complete("ID")
    
    check.config = configurations
    rs = SV.summary(check, df)
    assert isinstance(rs, DataFrame)
