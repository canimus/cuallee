from pyspark.sql import SparkSession
import pytest
from cuallee import Check, CheckLevel
import logging

@pytest.fixture(scope="session")
def spark():
    try:
        logger = logging.getLogger("py4j")
        logger.setLevel(logging.ERROR)
        spark_session = (
            SparkSession.builder
            .config("spark.driver.memory", "2g")
            .getOrCreate()
        )
        yield spark_session
    except:
        pass
    finally:
        spark_session.stop()

@pytest.fixture(scope="function")
def check():
    return Check(CheckLevel.WARNING, "PyTestCheck")

