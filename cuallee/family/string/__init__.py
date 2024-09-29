from cuallee.core.rule import Rule, RuleDataType
from typing import Dict
from abc import ABC, abstractmethod


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
