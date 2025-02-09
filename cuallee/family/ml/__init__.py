from abc import ABC, abstractmethod

from cuallee.core.rule import Rule, RuleDataType


class MlCheck(ABC):
    """Functionality for ml operations"""

    @abstractmethod
    def __init__(self) -> None:
        """Restrict use of MlCheck as it misses rule container"""
        pass

    def has_correlation(self, column_left: str, column_right: str, value: float):
        """
        Validates the correlation in a range of [0..1] between 2 columns

        Args:
            column_left (str): Column name in dataframe
            column_right (str): Column name in dataframe
            value (float): Value to match the correlation
        """
        (
            Rule(
                "has_correlation",
                [column_left, column_right],
                value,
                RuleDataType.NUMERIC,
            )
            >> self._rule
        )
        return self

    def has_entropy(self, column: str, value: float, tolerance: float = 0.01):
        """
        Validation for entropy calculation on continuous variables/features on `log2`.
        Useful in Machine Learning classifications to test imbalanced datasets with low entropy.

        Args:
            column (str): Column name in the dataframe
            value (float): The expected entropy value
            tolerance (float): The tolerance/precision used when comparing the actual and expected value

        Examples:

        """
        (
            Rule(
                "has_entropy",
                column,
                value,
                RuleDataType.AGNOSTIC,
                options=[tuple(["tolerance", tolerance])],
            )
            >> self._rule
        )
        return self

    def has_cardinality(self, column: str, value: int):
        """
        Validates the number of distinct values in a column

        Args:
            column (str): Column name in the dataframe
            value (int): The number of expected distinct values on a column
        """
        Rule("has_cardinality", column, value, RuleDataType.AGNOSTIC) >> self._rule
        return self

    def has_infogain(self, column: str, pct: float = 1.0):
        """
        Validate cardinality > 1.
        Particularly useful when validating categorical data for Machine Learning

        Args:
            column (str): Column name in the dataframe
            pct (float): The threshold percentage required to pass

        """
        (
            Rule(
                method="has_infogain",
                column=column,
                value="N/A",
                data_type=RuleDataType.AGNOSTIC,
                coverage=pct,
            )
            >> self._rule
        )
        return self
