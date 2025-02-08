import inspect
import logging
from abc import ABC, abstractmethod

import pytest
from pyspark.sql import SparkSession

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

    def get_data(self):
        """Get data for test using the caller's file location"""
        caller_frame = inspect.currentframe().f_back
        caller_file = caller_frame.f_globals["__file__"]
        return caller_file
