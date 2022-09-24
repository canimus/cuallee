import pyspark.sql.types as T
from pyspark.sql.dataframe import DataFrame
from typing import Collection, Union, Type, Dict

import exceptions as E


def _field_type_filter(
    dataframe: DataFrame,
    field_type: Union[Type[T.DateType], Type[T.NumericType], Type[T.TimestampType]],
) -> Collection:
    """Internal method to search for column names based on data type"""
    return set(
        [f.name for f in dataframe.schema.fields if isinstance(f.dataType, field_type)]  # type: ignore
    )


def numeric_fields(dataframe: DataFrame) -> Collection:
    """Filter all numeric data types in data frame and returns field names"""
    return _field_type_filter(dataframe, T.NumericType)


def string_fields(dataframe: DataFrame) -> Collection:
    """Filter all numeric data types in data frame and returns field names"""
    return _field_type_filter(dataframe, T.StringType)


def date_fields(dataframe: DataFrame) -> Collection:
    """Filter all date data types in data frame and returns field names"""
    return _field_type_filter(dataframe, T.DateType)


def timestamp_fields(dataframe: DataFrame) -> Collection:
    """Filter all date data types in data frame and returns field names"""
    return _field_type_filter(dataframe, T.TimestampType)


def column_datatype_validation(dictionary: Dict, dataframe: DataFrame, computation, value: int, target_type: str):
    target_column_set = set(
            [
                s if not isinstance(v.rule.column, str) else v.rule.column
                for v in dictionary.values()
                for s in v.rule.column
                if v.rule.data_type.value == value
            ]
        )
    target_fields = computation(dataframe)
    if target_column_set.issubset(target_fields):
        pass
    else:
        non_target_column = target_column_set.difference(target_fields)
        raise E.ColumnException(f"Column(s): {non_target_column} are not {target_type}")