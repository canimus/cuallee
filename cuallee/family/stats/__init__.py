from abc import ABC, abstractmethod
from typing import List

from cuallee.core.rule import Rule, RuleDataType


class StatsCheck(ABC):
    """Functionality for statistical validations"""

    @abstractmethod
    def __init__(self) -> None:
        """Restrict use of StatsCheck as it misses rule container"""
        pass

    def has_min(self, column: str, value: float, tolerance: float = 0.0):
        """
        Validation of a column's minimum value

        Args:
            column (str): Column name in dataframe
            value (number): The condition for the column to match
            tolerance (float): Precision for the value
        """

        (
            Rule(
                "has_min",
                column,
                value,
                RuleDataType.NUMERIC,
                options={"tolerance": tolerance},
            )
            >> self._rule
        )
        return self

    def has_max(self, column: str, value: float, tolerance: float = 0.0):
        """
        Validation of a column's maximum value

        Args:
            column (str): Column name in dataframe
            value (number): The condition for the column to match
            tolerance (float): Precision for the value
        """
        (
            Rule(
                "has_max",
                column,
                value,
                RuleDataType.NUMERIC,
                options={"tolerance": tolerance},
            )
            >> self._rule
        )
        return self

    def has_std(self, column: str, value: float, tolerance: float = 0.0):
        """
        Validation of a column's standard deviation

        Args:
            column (str): Column name in dataframe
            value (number): The condition for the column to match
            tolerance (float): Precision for the value
        """
        (
            Rule(
                "has_std",
                column,
                value,
                RuleDataType.NUMERIC,
                options={"tolerance": tolerance},
            )
            >> self._rule
        )
        return self

    def has_mean(self, column: str, value: float, tolerance: float = 0.0):
        """
        Validation of a column's average/mean

        Args:
            column (str): Column name in dataframe
            value (number): The condition for the column to match
            tolerance (float): Precision for the value
        """
        (
            Rule(
                "has_mean",
                column,
                value,
                RuleDataType.NUMERIC,
                options={"tolerance": tolerance},
            )
            >> self._rule
        )
        return self

    def has_sum(self, column: str, value: float, tolerance: float = 0.0):
        """
        Validation of a sum of all values of a column

        Args:
            column (str): Column name in dataframe
            value (number): The condition for the column to match
            tolerance (float): Precision for the value
        """
        (
            Rule(
                "has_sum",
                column,
                value,
                RuleDataType.NUMERIC,
                options={"tolerance": tolerance},
            )
            >> self._rule
        )
        return self

    def has_percentile(
        self, column: str, value: float, percentile: float, precision: int = 10000
    ):
        """
        Validation of a column percentile value using approximation

        Args:
            column (str): Column name in dataframe
            value (List[str,number,date]): The condition for the column to match
            percentile (float): Value between [0,1] i.e. `0.5` for median
            precision (float): The precision to calculate percentiles
        """
        (
            Rule(
                "has_percentile",
                column,
                value,
                RuleDataType.NUMERIC,
                options=[
                    tuple(["percentile", percentile]),
                    tuple(["precision", precision]),
                ],
            )
            >> self._rule
        )
        return self

    def is_inside_interquartile_range(
        self, column: str, value: List[float] = [0.25, 0.75], pct: float = 1.0
    ):
        """
        Validates a number resides inside the quartile(1) and quartile(3) of the range of values

        Args:
            column (str): Column name in dataframe
            value (List[number]): A number between 0 and 1 demarking the quartile
            pct (float): The threshold percentage required to pass
        """
        (
            Rule(
                "is_inside_interquartile_range",
                column,
                value,
                RuleDataType.NUMERIC,
                pct,
            )
            >> self._rule
        )
        return self
