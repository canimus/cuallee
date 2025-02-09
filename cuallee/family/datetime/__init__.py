from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Any, List, Tuple, Union

from cuallee.core.rule import Rule, RuleDataType


class DateTimeCheck(ABC):
    """Functionality for date or timestamp operations"""

    @abstractmethod
    def __init__(self) -> None:
        """Restrict use of DateTimeCheck as it misses rule container"""
        pass

    def is_t_minus_n(
        self,
        column: str,
        value: int,
        pct: float = 1.0,
    ):
        """
        Validate that date is `n` days before the current date

        Args:
            column (str): Column name in dataframe
            value (List[str,number,date]): The number of days before the current date
            pct (float): The threshold percentage required to pass
        """
        target_day = datetime.now(tz=timezone.utc) - timedelta(days=value)
        return self.is_contained_in(
            column,
            tuple([target_day.strftime("%Y-%m-%d")]),
            [RuleDataType.DATE, RuleDataType.TIMESTAMP],
            pct,
            options={"name": f"is_t_minus_{value}"},
        )

    def is_t_minus_1(self, column: str, pct: float = 1.0):
        """
        Validate that date is yesterday

        Args:
            column (str): Column name in dataframe
            pct (float): The threshold percentage required to pass
        """
        return self.is_t_minus_n(column, 1, pct, options={"name": "is_t_minus_1"})

    def is_yesterday(self, column: str, pct: float = 1.0):
        """
        Validate that date is yesterday

        Args:
            column (str): Column name in dataframe
            pct (float): The threshold percentage required to pass
        """
        return self.is_t_minus_n(column, 1, pct, options={"name": "is_yesterday"})

    def is_t_minus_2(self, column: str, pct: float = 1.0):
        """
        Validate that date is 2 days ago

        Args:
            column (str): Column name in dataframe
            pct (float): The threshold percentage required to pass
        """
        return self.is_t_minus_n(column, 2, pct, options={"name": "is_t_minus_2"})

    def is_t_minus_3(self, column: str, pct: float = 1.0):
        """
        Validate that date is 3 days ago

        Args:
            column (str): Column name in dataframe
            pct (float): The threshold percentage required to pass
        """
        return self.is_t_minus_n(column, 3, pct, options={"name": "is_t_minus_3"})

    def is_today(self, column: str, pct: float = 1.0):
        """
        Validate that date is today

        Args:
            column (str): Column name in dataframe
            pct (float): The threshold percentage required to pass
        """
        return self.is_t_minus_n(column, 0, pct, options={"name": "is_today"})

    def is_on_weekday(self, column: str, value: str = "Mon-Fri", pct: float = 1.0):
        """
        Validates a datetime column is in a Mon-Fri time range (default)

        Args:
            column (str): Column name in the dataframe
            pct (float): The threshold percentage required to pass
        """
        (
            Rule(
                "is_on_weekday",
                column,
                value,
                [RuleDataType.DATE, RuleDataType.TIMESTAMP],
                pct,
            )
            >> self._rule
        )
        return self

    def is_on_weekend(self, column: str, value: str = "Sat-Sun", pct: float = 1.0):
        """
        Validates a datetime column is in a Sat-Sun time range (default)

        Args:
            column (str): Column name in the dataframe
            pct (float): The threshold percentage required to pass
        """
        (
            Rule(
                "is_on_weekend",
                column,
                value,
                [RuleDataType.DATE, RuleDataType.TIMESTAMP],
                pct,
            )
            >> self._rule
        )
        return self

    def is_on_monday(self, column: str, pct: float = 1.0):
        """
        Validates a datetime column is on Monday

        Args:
            column (str): Column name in the dataframe
            pct (float): The threshold percentage required to pass
        """
        (
            Rule(
                "is_on_monday",
                column,
                "Mon",
                [RuleDataType.DATE, RuleDataType.TIMESTAMP],
                pct,
            )
            >> self._rule
        )
        return self

    def is_on_tuesday(self, column: str, pct: float = 1.0):
        """
        Validates a datetime column is on Tuesday

        Args:
            column (str): Column name in the dataframe
            pct (float): The threshold percentage required to pass
        """
        (
            Rule(
                "is_on_tuesday",
                column,
                "Tue",
                [RuleDataType.DATE, RuleDataType.TIMESTAMP],
                pct,
            )
            >> self._rule
        )
        return self

    def is_on_wednesday(self, column: str, pct: float = 1.0):
        """
        Validates a datetime column is on Wednesday

        Args:
            column (str): Column name in the dataframe
            pct (float): The threshold percentage required to pass
        """
        (
            Rule(
                "is_on_wednesday",
                column,
                "Wed",
                [RuleDataType.DATE, RuleDataType.TIMESTAMP],
                pct,
            )
            >> self._rule
        )
        return self

    def is_on_thursday(self, column: str, pct: float = 1.0):
        """
        Validates a datetime column is on Thursday

        Args:
            column (str): Column name in the dataframe
            pct (float): The threshold percentage required to pass
        """
        (
            Rule(
                "is_on_thursday",
                column,
                "Thu",
                [RuleDataType.DATE, RuleDataType.TIMESTAMP],
                pct,
            )
            >> self._rule
        )
        return self

    def is_on_friday(self, column: str, pct: float = 1.0):
        """
        Validates a datetime column is on Friday

        Args:
            column (str): Column name in the dataframe
            pct (float): The threshold percentage required to pass
        """
        (
            Rule(
                "is_on_friday",
                column,
                "Fri",
                [RuleDataType.DATE, RuleDataType.TIMESTAMP],
                pct,
            )
            >> self._rule
        )
        return self

    def is_on_saturday(self, column: str, pct: float = 1.0):
        """
        Validates a datetime column is on Saturday

        Args:
            column (str): Column name in the dataframe
            pct (float): The threshold percentage required to pass
        """
        (
            Rule(
                "is_on_saturday",
                column,
                "Sat",
                [RuleDataType.DATE, RuleDataType.TIMESTAMP],
                pct,
            )
            >> self._rule
        )
        return self

    def is_on_sunday(self, column: str, pct: float = 1.0):
        """
        Validates a datetime column is on Sunday

        Args:
            column (str): Column name in the dataframe
            pct (float): The threshold percentage required to pass
        """
        (
            Rule(
                "is_on_sunday",
                column,
                "Sun",
                [RuleDataType.DATE, RuleDataType.TIMESTAMP],
                pct,
            )
            >> self._rule
        )
        return self

    def is_on_schedule(self, column: str, value: Tuple[Any], pct: float = 1.0):
        """
        Validation of a datetime column between an hour interval

        Args:
            column (str): Column name in the dataframe
            value (Tuple[int,int]): A tuple indicating a 24hr day interval. i.e. (9,17) for 9am to 5pm
            pct (float): The threshold percentage required to pass
        """
        (
            Rule("is_on_schedule", column, value, RuleDataType.TIMESTAMP, pct)
            >> self._rule
        )
        return self

    def is_daily(
        self, column: str, value: Union[None, List[int]] = None, pct: float = 1.0
    ):
        """
        Validates that there is no missing dates using only week days in the date/timestamp column.

        An alternative day combination can be provided given that a user wants to validate only certain dates.
        For example in PySpark to validate that time series are every Wednesday consecutively on a year
        without any missing values, the value input should contain `[4]` as it represent the numeric
        equivalence of the day of week Wednesday.

        Args:
            column (str): Column name in the dataframe
            value (List[int]): A list of numbers describing the days of the week to consider. i.e. Pyspark uses [2, 3, 4, 5, 6] for Mon-Fri
            pct (float): The threshold percentage required to pass
        """
        (
            Rule(
                "is_daily",
                column,
                value,
                [RuleDataType.DATE, RuleDataType.TIMESTAMP],
                pct,
            )
            >> self._rule
        )
        return self
