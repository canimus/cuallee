from abc import ABC, abstractmethod

from cuallee.core.rule import Rule, RuleDataType


class NumericCheck(ABC):
    """Functionality for numeric and algebra operations"""

    @abstractmethod
    def __init__(self) -> None:
        """Restrict use of NumericCheck as it misses rule container"""
        pass

    def is_greater_than(
        self,
        column: str,
        value: float,
        pct: float = 1.0,
    ):
        """
        Validation for numeric greater than value

        Args:
            column (str): Column name in dataframe
            value (number): The condition for the column to match
            pct (float): The threshold percentage required to pass
        """
        (
            Rule(
                "is_greater_than",
                column,
                value,
                [RuleDataType.NUMERIC, RuleDataType.DATE, RuleDataType.TIMESTAMP],
                pct,
            )
            >> self._rule
        )
        return self

    def gt(self, column: str, value: float, pct: float = 1.0):
        """
        Validation for numeric greater than value
        Alias for is_greater_than

        Args:
            column (str): Column name in dataframe
            value (number): The condition for the column to match
            pct (float): The threshold percentage required to pass
        """
        (
            Rule(
                "is_greater_than",
                column,
                value,
                [RuleDataType.NUMERIC, RuleDataType.DATE, RuleDataType.TIMESTAMP],
                pct,
                options={"name": "gt"},
            )
            >> self._rule
        )
        return self

    def is_above(self, column: str, value: float, pct: float = 1.0):
        """
        Validation for numeric greater than value
        Alias for is_greater_than

        Args:
            column (str): Column name in dataframe
            value (number): The condition for the column to match
            pct (float): The threshold percentage required to pass
        """
        (
            Rule(
                "is_greater_than",
                column,
                value,
                [RuleDataType.NUMERIC, RuleDataType.DATE, RuleDataType.TIMESTAMP],
                pct,
                options={"name": "is_above"},
            )
            >> self._rule
        )
        return self

    def is_positive(self, column: str, pct: float = 1.0):
        """
        Validation for numeric greater than zero

        Args:
            column (str): Column name in dataframe
            pct (float): The threshold percentage required to pass
        """
        return self.is_greater_than(
            column, 0, RuleDataType.NUMERIC, pct, options={"name": "is_positive"}
        )

    def is_greater_or_equal_than(self, column: str, value: float, pct: float = 1.0):
        """
        Validation for numeric greater or equal than value

        Args:
            column (str): Column name in dataframe
            value (number): The condition for the column to match
            pct (float): The threshold percentage required to pass
        """
        (
            Rule(
                "is_greater_or_equal_than",
                column,
                value,
                [RuleDataType.NUMERIC, RuleDataType.DATE, RuleDataType.TIMESTAMP],
                pct,
            )
            >> self._rule
        )
        return self

    def ge(self, column: str, value: float, pct: float = 1.0):
        """
        Validation for numeric greater or equal than value
        Alias for is_greater_or_equal_than

        Args:
            column (str): Column name in dataframe
            value (number): The condition for the column to match
            pct (float): The threshold percentage required to pass
        """
        (
            Rule(
                "is_greater_or_equal_than",
                column,
                value,
                [RuleDataType.NUMERIC, RuleDataType.DATE, RuleDataType.TIMESTAMP],
                pct,
                options={"name": "ge"},
            )
            >> self._rule
        )
        return self

    def is_in_millions(self, column: str, pct: float = 1.0):
        """
        Validates that a column has values greater than 1M (1e6)

        Args:
            column (str): Column name in dataframe
            pct (float): The threshold percentage required to pass
        """
        return self.is_greater_or_equal_than(
            column, 1e6, RuleDataType.NUMERIC, pct, options={"name": "is_in_millions"}
        )

    def is_in_billions(self, column: str, pct: float = 1.0):
        """
        Validates that a column has values greater than 1B (1e9)

        Args:
            column (str): Column name in dataframe
            pct (float): The threshold percentage required to pass
        """
        return self.is_greater_or_equal_than(
            column, 1e9, RuleDataType.NUMERIC, pct, options={"name": "is_in_billions"}
        )

    def is_less_than(self, column: str, value: float, pct: float = 1.0):
        """
        Validation for numeric less than value

        Args:
            column (str): Column name in dataframe
            value (number): The condition for the column to match
            pct (float): The threshold percentage required to pass
        """
        (
            Rule(
                "is_less_than",
                column,
                value,
                [RuleDataType.NUMERIC, RuleDataType.DATE, RuleDataType.TIMESTAMP],
                pct,
            )
            >> self._rule
        )
        return self

    def lt(self, column: str, value: float, pct: float = 1.0):
        """
        Validation for numeric less than value
        Alias for is_less_than

        Args:
            column (str): Column name in dataframe
            value (number): The condition for the column to match
            pct (float): The threshold percentage required to pass
        """
        (
            Rule(
                "is_less_than",
                column,
                value,
                [RuleDataType.NUMERIC, RuleDataType.DATE, RuleDataType.TIMESTAMP],
                pct,
                options={"name": "lt"},
            )
            >> self._rule
        )
        return self

    def is_below(self, column: str, value: float, pct: float = 1.0):
        """
        Validation for numeric less than value
        Alias for is_less_than

        Args:
            column (str): Column name in dataframe
            value (number): The condition for the column to match
            pct (float): The threshold percentage required to pass
        """
        (
            Rule(
                "is_less_than",
                column,
                value,
                [RuleDataType.NUMERIC, RuleDataType.DATE, RuleDataType.TIMESTAMP],
                pct,
                options={"name": "lt"},
            )
            >> self._rule
        )
        return self

    def is_negative(self, column: str, pct: float = 1.0):
        """
        Validation for numeric less than zero

        Args:
            column (str): Column name in dataframe
            pct (float): The threshold percentage required to pass
        """
        return self.is_less_than(
            column, 0, RuleDataType.NUMERIC, pct, options={"name": "is_negative"}
        )

    def is_less_or_equal_than(self, column: str, value: float, pct: float = 1.0):
        """
        Validation for numeric less or equal than value

        Args:
            column (str): Column name in dataframe
            value (number): The condition for the column to match
            pct (float): The threshold percentage required to pass
        """
        (
            Rule(
                "is_less_or_equal_than",
                column,
                value,
                [RuleDataType.NUMERIC, RuleDataType.DATE, RuleDataType.TIMESTAMP],
                pct,
            )
            >> self._rule
        )
        return self

    def le(self, column: str, value: float, pct: float = 1.0):
        """
        Validation for numeric less or equal than value
        Alias for is_less_or_equal_than

        Args:
            column (str): Column name in dataframe
            value (number): The condition for the column to match
            pct (float): The threshold percentage required to pass
        """
        (
            Rule(
                "is_less_or_equal_than",
                column,
                value,
                [RuleDataType.NUMERIC, RuleDataType.DATE, RuleDataType.TIMESTAMP],
                pct,
                options={"name": "le"},
            )
            >> self._rule
        )
        return self

    def is_equal_than(self, column: str, value: float, pct: float = 1.0):
        """
        Validation for numeric column equal than value

        Args:
            column (str): Column name in dataframe
            value (number): The condition for the column to match
            pct (float): The threshold percentage required to pass
        """
        (
            Rule(
                "is_equal_than",
                column,
                value,
                [RuleDataType.NUMERIC, RuleDataType.DATE, RuleDataType.TIMESTAMP],
                pct,
            )
            >> self._rule
        )
        return self

    def eq(self, column: str, value: float, pct: float = 1.0):
        """
        Validation for numeric column equal than value
        Alias for is_equal_than

        Args:
            column (str): Column name in dataframe
            value (number): The condition for the column to match
            pct (float): The threshold percentage required to pass
        """
        (
            Rule(
                "is_equal_than",
                column,
                value,
                [RuleDataType.NUMERIC, RuleDataType.DATE, RuleDataType.TIMESTAMP],
                pct,
                options={"name": "eq"},
            )
            >> self._rule
        )
        return self
