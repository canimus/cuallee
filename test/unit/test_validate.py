import pytest
import pandas as pd
import  pyspark.sql.functions as F
from pyspark.sql import DataFrame
from cuallee import Check, CheckLevel
import cuallee.dataframe as D
import cuallee.exceptions as E
from loguru import logger


def test_empty_dictionary(spark):
    df = spark.range(10).alias('id')
    with pytest.raises(AssertionError) as e:
        Check(CheckLevel.WARNING, "test_empty_observation").validate(spark, df)
        assert "Check is empty. Add validations i.e. is_complete, is_unique, etc." == str(e)


def test_pandas_dataframe(spark):
    df = pd.DataFrame({'id': [1, 2], 'desc': ['1', '2']})
    with pytest.raises(AssertionError) as e:
        Check(CheckLevel.WARNING, "test_empty_observation").is_complete('id').validate(spark, df)
        assert "Cualle operates only with Spark Dataframes" == str(e)


def test_column_name_validation(spark):
    df = spark.range(10).alias('id')
    with pytest.raises(E.ColumnException) as e:
        Check(CheckLevel.WARNING, "test_empty_observation").is_complete('ide').validate(spark, df)
        assert "Column(s): ide not in dataframe" == str(e)


def test_numeric_column_validation(spark):
    df = spark.range(10).alias('id').withColumn('desc', F.lit(F.col('id').cast('string')))
    c = Check(CheckLevel.WARNING, "test_numeric_column").has_min('id', 3).has_min('desc', 3)
    rules = {**c._unique, **c._compute}
    with pytest.raises(E.ColumnException) as e:
        D.column_datatype_validation(rules, df, D.numeric_fields, 1, 'numeric')
        assert "Column(s): desc are not numeric" == str(e)

def test_string_column_validation(spark):
    df = spark.range(10).alias('id').withColumn('desc', F.lit(F.col('id').cast('string')))
    c = Check(CheckLevel.WARNING, "test_string_column").matches_regex('id', 'reg').matches_regex('desc', 'reg')
    rules = {**c._unique, **c._compute}
    with pytest.raises(E.ColumnException) as e:
        D.column_datatype_validation(rules, df, D.string_fields, 2, 'string')
        assert "Column(s): id are not string" == str(e)

def test_date_column_validation(spark):
    pass


def test_timestamp_column_validation(spark):
    pass

def test_observe_no_compute(spark):
    df = spark.range(10).alias('id').withColumn('desc', F.lit(F.col('id').cast('string')))
    rs = Check(CheckLevel.WARNING, "test_empty_observation").is_unique('id').validate(spark, df)
    assert isinstance(rs, DataFrame)