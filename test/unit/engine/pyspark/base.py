import inspect
import logging
from abc import ABC, abstractmethod

import pyspark.sql.functions as F
import pytest
from pyspark.sql import DataFrame, SparkSession

from cuallee.core.check import Check, CheckLevel

logger = logging.getLogger(__name__)


class PysparkTestCase(ABC):
    """Base class defining required test methods for validation rules"""

    @pytest.fixture(scope="session")
    def spark(self):
        try:
            logger = logging.getLogger("py4j")
            logger.setLevel(logging.ERROR)
            spark_session = (
                SparkSession.builder.config("spark.driver.memory", "2g")
                .config("spark.security.manager.allow", "true")
                .getOrCreate()
            )
            yield spark_session
        except Exception:
            pass
        finally:
            spark_session.stop()

    @pytest.fixture(scope="function")
    def check(self):
        return Check(CheckLevel.WARNING, "PysparkCheck")

    @abstractmethod
    def test_pass(self):
        """Test case for successful validation"""
        pass

    @abstractmethod
    def test_fail(self):
        """Test case for failed validation"""
        pass

    @abstractmethod
    def test_exception(self):
        """Test case for exception handling"""
        pass

    @abstractmethod
    def test_parameter(self):
        """Test case for parameter variations"""
        pass

    @abstractmethod
    def test_coverage(self):
        """Test case for coverage calculation"""
        pass

    def get_data(self, spark_session: SparkSession) -> DataFrame:
        """Get data for test using the caller's file location"""
        frame = inspect.currentframe().f_back
        caller_file = frame.f_globals["__file__"]
        calling_function = frame.f_code.co_name[5:]
        test_name = caller_file.split("/")[-1][5:-3]

        df = spark_session.read.parquet("test/fixtures/pyspark.parquet")
        mappings = {"pass": ["id_1"], "fail": ["id_3"]}
        df = (
            df.filter(F.col("check") == test_name)
            # .filter(F.col("test") == calling_function)
            .select(*mappings[calling_function])
        )
        return df
