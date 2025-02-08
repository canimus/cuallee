import inspect
import logging
from abc import ABC, abstractmethod

import pytest

from cuallee.core.check import Check, CheckLevel

logger = logging.getLogger(__name__)


class PandasTestCase(ABC):
    """Base class defining required test methods for validation rules"""

    @pytest.fixture(scope="function")
    def check(self):
        return Check(CheckLevel.WARNING, "PandasCheck")

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
