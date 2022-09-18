from pyspark.sql import SparkSession, DataFrame, Row, Observation
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pytest


@pytest.fixture
def spark():
    return SparkSession.builder.getOrCreate()