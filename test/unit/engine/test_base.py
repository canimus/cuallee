from abc import ABC, abstractmethod

import pandas as pd
import polars as pl
import pytest
from pyspark.sql import SparkSession

from cuallee.core.check import ENGINES, Check, CheckLevel


class CualleeTest(ABC):

    @pytest.fixture
    def check(self):
        return Check(
            level=CheckLevel.WARNING,
            name="test_check",
            table_name="test_table",
            config={"key": "value"},
        )

    @pytest.fixture
    def engine(self):
        return ENGINES

    def data(self, engine):
        if engine == "pandas":
            return pd.read_parquet("test/fixtures/unit_data.parquet")
        elif engine == "polars":
            return pl.read_parquet("test/fixtures/unit_data.parquet")
        elif engine == "pyspark":
            spark = SparkSession.builder.getOrCreate()
            return spark.read.parquet("test/fixtures/unit_data.parquet")

    # ------------ TEST SKELETONS ---------------
    # @pytest.mark.parametrize("engine", ENGINES)
    @pytest.mark.parametrize("engine", ["pyspark"])
    def test_positive(self, check, engine):
        data = self.data(engine)
        self.logic_positive(check, data)

    # @pytest.mark.parametrize("engine", ENGINES)
    @pytest.mark.parametrize("engine", ["pyspark"])
    def test_negative(self, check, engine):
        data = self.data(engine)
        self.logic_negative(check, data)

    @pytest.fixture
    def parameters(self, request):
        """Dynamic fixture for parameter handling"""
        config = self.get_parameter_config()
        if isinstance(config, str):
            pytest.skip(config)
        if config:
            return request.param
        return None

    @pytest.mark.parametrize("engine", ["pyspark"])
    def test_parameters(self, check, engine, parameters):
        """Run parameterized tests with custom parameters"""
        data = self.data(engine)

        # Get parameter configuration
        config = self.get_parameter_config()
        if isinstance(config, str):
            pytest.skip(config)

        self.logic_parameters(check, data, parameters)

    def pytest_generate_tests(self, metafunc):
        """Dynamically generate parameters if needed"""
        if "parameters" in metafunc.fixturenames:
            config = self.get_parameter_config()
            if isinstance(config, dict):
                metafunc.parametrize(
                    "parameters",
                    [
                        tuple(v) if isinstance(v, list) else v
                        for v in config["argvalues"]
                    ],
                    ids=config.get("ids", None),
                )

    # @pytest.mark.parametrize("engine", ENGINES)
    @pytest.mark.parametrize("engine", ["pyspark"])
    def test_coverage(self, check, engine):
        data = self.data(engine)
        self.logic_coverage(check, data)

    # ------------ ABSTRACT METHODS FOR LOGIC ---------------
    @abstractmethod
    def logic_positive(self, check, data): ...

    @abstractmethod
    def logic_negative(self, check, data): ...

    # @abstractmethod
    # def logic_parameters(self, check, data, param): ...
    @abstractmethod
    def logic_parameters(self, check, data, parameters): ...

    @abstractmethod
    def get_parameter_config(self):
        """
        Override this method to provide custom parameter configuration
        Returns:
            dict: pytest.mark.parametrize configuration or string
        """
        ...

    @abstractmethod
    def logic_coverage(self, check, data): ...
