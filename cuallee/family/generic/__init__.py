from cuallee.core.rule import Rule, RuleDataType
from typing import Union, List, Tuple


class GenericCheck:
    """Functionality for completeness and uniqueness validations"""

    def is_complete(self, column: str, pct: float = 1.0):
        """
        Validation for non-null values in column

        Args:
            column (str): Column name in dataframe
            pct (float): The threshold percentage required to pass

        """
        Rule("is_complete", column, "N/A", RuleDataType.AGNOSTIC, pct) >> self._rule
        return self

    def is_empty(self, column: str, pct: float = 1.0):
        """
        Validation for null values in column

        Args:
            column (str): Column name in dataframe
            pct (float): The threshold percentage required to pass

        """
        Rule("is_empty", column, "N/A", RuleDataType.AGNOSTIC, pct) >> self._rule
        return self

    def are_complete(self, column: Union[List[str], Tuple[str, str]], pct: float = 1.0):
        """
        Validation for non-null values in a group of columns

        Args:
            column (List[str]): A tuple or list of column names in dataframe
            pct (float): The threshold percentage required to pass
        """
        Rule("are_complete", column, "N/A", RuleDataType.AGNOSTIC, pct) >> self._rule
        return self

    def is_unique(
        self,
        column: str,
        pct: float = 1.0,
        approximate: bool = False,
        ignore_nulls: bool = False,
    ):
        """
        Validation for unique values in column

        Args:
            column (str): Column name in dataframe
            pct (float): The threshold percentage required to pass
            approximate (bool): A flag to speed up computation using an approximation through maximum relative std. dev.
            ignore_nulls (bool): Run drop nulls before counting
        """
        (
            Rule(
                "is_unique",
                column,
                "N/A",
                RuleDataType.AGNOSTIC,
                pct,
                options={"approximate": approximate, "ignore_nulls": ignore_nulls},
            )
            >> self._rule
        )
        return self

    def is_primary_key(self, column: str, pct: float = 1.0):
        """
        Validation for unique values in column

        Args:
            column (str): Column name in dataframe
            pct (float): The threshold percentage required to pass
        """
        (
            Rule(
                "is_unique",
                column,
                "N/A",
                RuleDataType.AGNOSTIC,
                pct,
                options={"name": "is_primary_key"},
            )
            >> self._rule
        )
        return self

    def are_unique(self, column: Union[List[str], Tuple[str, str]], pct: float = 1.0):
        """
        Validation for unique values in a group of columns

        Args:
            column (List[str]): A tuple or list of column names in dataframe
            pct (float): The threshold percentage required to pass
        """
        Rule("are_unique", column, "N/A", RuleDataType.AGNOSTIC, pct) >> self._rule
        return self

    def is_composite_key(
        self, column: Union[List[str], Tuple[str, str]], pct: float = 1.0
    ):
        """
        Validation for unique values in a group of columns

        Args:
            column (str): Column name in dataframe
            pct (float): The threshold percentage required to pass
        """
        (
            Rule(
                "are_unique",
                column,
                "N/A",
                RuleDataType.AGNOSTIC,
                pct,
                options={"name": "is_composite_key"},
            )
            >> self._rule
        )
        return self
