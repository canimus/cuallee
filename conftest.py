from pyspark.sql import SparkSession
import pytest
from cuallee import Check, CheckLevel


@pytest.fixture(scope="session")
def spark():
    try:
        spark_session = SparkSession.builder.config("spark.driver.memory", "2g").getOrCreate()
        yield spark_session
    except:
        pass
    finally:
        spark_session.stop()

@pytest.fixture(scope="function")
def check():
    return Check(CheckLevel.WARNING, "PyTestCheck")

