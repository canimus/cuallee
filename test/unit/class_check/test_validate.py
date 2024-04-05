import daft
import pytest
import pandas as pd
from pyspark.sql import DataFrame
from google.cloud import bigquery
from cuallee import Check, CheckLevel
from polars import DataFrame as polars_dataframe


# __ SPARK DATAFRAME TESTS __


def test_return_spark_dataframe(spark):
    df = spark.range(10).alias("id")
    rs = (
        Check(CheckLevel.WARNING, "test_spark_dataframe").is_complete("id").validate(df)
    )
    assert isinstance(rs, DataFrame)


def test_empty_dictionary_spark(spark):
    df = spark.range(10).alias("id")
    with pytest.raises(
        Exception,
        match="Check is empty",
    ):
        Check(CheckLevel.WARNING, "test_empty_observation_spark").validate(df)


def test_column_name_validation_spark(spark):
    df = spark.range(10).alias("id")
    with pytest.raises(Exception, match="not present in dataframe"):
        Check(CheckLevel.WARNING, "test_column_name_spark").is_complete("ide").validate(
            df
        )


# def test_order_validate_args(spark):
#     df = spark.range(10).alias("id")
#     with pytest.raises(
#         AttributeError, match="'SparkSession' object has no attribute 'columns'"
#     ):
#         Check(CheckLevel.WARNING, "test_order_validate_args").is_complete(
#             "id"
#         ).validate(df)


# def test_spark_session_in_arg(spark):
#     df = spark.range(10).alias("id")
#     with pytest.raises(
#         Exception,
#         match="The function requires to pass a spark session as arg",
#     ):
#         Check(CheckLevel.WARNING, "test_spark_session_in_arg").is_complete(
#             "id"
#         ).validate(df, "spark")


# def test_update_rule_status_spark(spark):
#     df = spark.range(10).alias("id")
#     rs = Check(CheckLevel.WARNING, "test_spark_session_in_arg").is_complete("id")
#     for v in rs._rule.values():
#         assert v.status == None
#     rs.validate(df)
#     for v in rs._rule.values():
#         assert isinstance(v.status, str)
#         assert v.status == "PASS"


# __ PANDAS DATAFRAME TESTS __


def test_return_pandas_dataframe():
    df = pd.DataFrame({"id": [1, 2], "desc": ["1", "2"]})
    rs = (
        Check(CheckLevel.WARNING, "test_column_name_pandas")
        .is_complete("id")
        .validate(df)
    )
    # assert isinstance(rs, pd.DataFrame)
    pass  # To complete when pandas_validation.py done


def test_empty_dictionary_pandas():
    df = pd.DataFrame({"id": [1, 2], "desc": ["1", "2"]})
    with pytest.raises(
        Exception,
        match="Check is empty",
    ):
        Check(CheckLevel.WARNING, "test_empty_observation_pandas").validate(df)


def test_column_name_validation_pandas():
    df = pd.DataFrame({"id": [1, 2], "desc": ["1", "2"]})
    with pytest.raises(Exception) as e:
        Check(CheckLevel.WARNING, "test_empty_observation").is_complete("ide").validate(
            df
        )
        assert "Column(s): ide not in dataframe" == str(e)


# __ Daft DATAFRAME TESTS __


def test_return_daft_dataframe():
    df = daft.from_pydict({"id": [1, 2], "desc": ["1", "2"]})
    rs = (
        Check(CheckLevel.WARNING, "test_column_name_pandas")
        .is_complete("id")
        .validate(df)
    )
    assert isinstance(rs, daft.DataFrame)


def test_empty_dictionary_daft():
    df = daft.from_pydict({"id": [1, 2], "desc": ["1", "2"]})
    with pytest.raises(
        Exception,
        match="Check is empty",
    ):
        Check(CheckLevel.WARNING, "test_empty_observation_pandas").validate(df)


def test_column_name_validation_daft():
    df = daft.from_pydict({"id": [1, 2], "desc": ["1", "2"]})
    with pytest.raises(Exception) as e:
        Check(CheckLevel.WARNING, "test_empty_observation").is_complete("ide").validate(
            df
        )
        assert "Column(s): ide not in dataframe" == str(e)


# __ DB Tables TESTS __

def test_return_mysql_dataframe(check: Check, db_conn_mysql):
    check.is_complete("id")
    check.table_name = "public.test1"
    result = check.validate(db_conn_mysql)
    assert isinstance(result, polars_dataframe)

def test_return_psql_dataframe(check: Check, postgresql, db_conn_psql):
    check.is_complete("id")
    check.table_name = "public.test1"
    result = check.validate(db_conn_psql)
    assert isinstance(result, polars_dataframe)


def test_empty_dictionary_mysql(db_conn_mysql):
    with pytest.raises(
        Exception,
        match="Check is empty",
    ):
        Check(CheckLevel.WARNING, "test_empty_observation_db").validate(db_conn_mysql)

def test_empty_dictionary_psql(postgresql, db_conn_psql):
    with pytest.raises(
        Exception,
        match="Check is empty",
    ):
        Check(CheckLevel.WARNING, "test_empty_observation_db").validate(db_conn_psql)


def test_column_name_validation_mysql(check: Check, db_conn_mysql):
    with pytest.raises(Exception) as e:
        check.is_complete("ide")
        check.table_name = "public.test1"
        check.validate(db_conn_mysql)
    assert "MySqlError { ERROR 1054 (42S22): Unknown column 'ide' in 'field list' }" in str(e.value)


def test_column_name_validation_psql(check: Check, postgresql, db_conn_psql):
    with pytest.raises(Exception) as e:
        check.is_complete("ide")
        check.table_name = "public.test1"
        check.validate(db_conn_psql)
        print(str(e.value))
    assert 'db error: ERROR: column "ide" does not exist' in str(e.value)

# __ BIGQUERY TESTS __
# def test_validate_bigquery(bq_client):
#     df = bigquery.dataset.Table('bigquery-public-data.chicago_taxi_trips.taxi_trips')
#     rs = (
#         Check(CheckLevel.WARNING, "test_spark_dataframe").is_complete("taxi_id").validate(df)
#     )
#     assert isinstance(rs, pd.DataFrame)
