import pyspark.sql.types as T
from pyspark.sql.dataframe import DataFrame
from typing import Collection, List


def _field_type_filter(dataframe: DataFrame, data_type: T.DataType) -> Collection:
    """Internal method to search for column names based on data type"""
    return set(
        [f.name for f in dataframe.schema.fields if isinstance(f.dataType, data_type)]
    )


def numeric_fields(dataframe: DataFrame) -> Collection:
    """Filter all numeric data types in data frame and returns field names"""
    return _field_type_filter(dataframe, T.NumericType)


def date_fields(dataframe: DataFrame) -> Collection:
    """Filter all date data types in data frame and returns field names"""
    return _field_type_filter(dataframe, T.DateType)


def timestamp_fields(dataframe: DataFrame) -> Collection:
    """Filter all date data types in data frame and returns field names"""
    return _field_type_filter(dataframe, T.TimestampType)
