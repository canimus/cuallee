from abc import ABC, abstractmethod

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
