from abc import ABC, abstractmethod

import duckdb
import pandas as pd
import pytest

from cuallee.core.check import Check, CheckLevel


class DuckDbTestCase(ABC):
    @pytest.fixture(scope="function")
    def db(self):
        try:
            conn = duckdb.connect(":memory:")
            conn.execute(
                """
                CREATE OR REPLACE TABLE TEMP AS (
                    SELECT * FROM range(10) t(ID),
                    LATERAL (SELECT ID + 1) t2(ID2),
                    LATERAL (SELECT CASE WHEN ID < 5 THEN ID ELSE NULL END) t3(ID3),
                    LATERAL (SELECT CASE WHEN ID < 5 THEN 0 ELSE ID + 100 END) t4(ID4)
                )
            """
            )

            conn.execute(
                """
                CREATE OR REPLACE TABLE DUPS AS (
                    select * FROM TEMP
                    UNION ALL
                    select * FROM TEMP
                )
            """
            )

            yield conn
        except Exception:
            pass
        finally:
            conn.close()

    @pytest.fixture(scope="function")
    def check(self):
        return Check(level=CheckLevel.WARNING, name="DuckDbCheck", table_name="TEMP")

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

    def evaluate(self, dataframe: pd.DataFrame, passed=True) -> bool:
        """
        Evaluates if the 'status' column in the dataframe contains only the string 'PASS'.

        Parameters:
        df (pd.DataFrame): The dataframe to evaluate.

        Returns:
        bool: True if all values in the 'status' column are 'PASS', False otherwise.
        """
        expected_status = "PASS" if passed else "FAIL"
        return dataframe.status.eq(expected_status).all().item()
