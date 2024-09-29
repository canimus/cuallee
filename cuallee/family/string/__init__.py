from abc import ABC, abstractmethod
from typing import Dict

from cuallee.core.rule import Rule, RuleDataType


class StringCheck(ABC):
    """Functionality for string pattern validations"""

    @abstractmethod
    def __init__(self) -> None:
        """Restrict use of StringCheck as it misses rule container"""
        pass

    def has_pattern(
        self, column: str, value: str, pct: float = 1.0, options: Dict[str, str] = {}
    ):
        """
        Validation for string type column matching regex expression

        Args:
            column (str): Column name in dataframe
            value (regex): A regular expression used to  match values in the `column`
            pct (float): The threshold percentage required to pass
        """
        (
            Rule(
                "has_pattern", column, value, RuleDataType.STRING, pct, options=options
            )
            >> self._rule
        )
        return self

    def is_legit(self, column: str, pct: float = 1.0):
        """
        Validation for string columns giving wrong signal about completeness due to empty strings.

        Useful for reading CSV files and preventing empty strings being reported as valid records.
        This is an `alias` implementation of the `has_pattern` rule that validates single words without spaces.

        Args:
            column (str): Column name in dataframe
            pct (float): The threshold percentage required to pass
        """
        (
            Rule(
                "has_pattern",
                column,
                r"^\S+$",
                RuleDataType.STRING,
                pct,
                options={"name": "is_legit"},
            )
            >> self._rule
        )
        return self
