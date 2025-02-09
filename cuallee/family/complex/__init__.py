from abc import ABC, abstractmethod
from typing import Callable, Dict, List, Tuple, Union

from cuallee.core.rule import Rule, RuleDataType


class ComplexCheck(ABC):
    """Functionality for operations involving multiple columns and/or rows"""

    @abstractmethod
    def __init__(self) -> None:
        """Restrict use of ComplexCheck as it misses rule container"""
        pass

    def has_max_by(
        self, column_source: str, column_target: str, value: Union[float, str]
    ):
        """
        Validation the correspondance of a column value based on another column maximum

        Args:
            column_source (str): Column used to obtain the row with the max value
            column_target (str): Column used to verify the matching value
            value (str,number): The value to match against
        """
        (
            Rule(
                "has_max_by",
                [column_source, column_target],
                value,
                RuleDataType.DUO,
            )
            >> self._rule
        )
        return self

    def has_min_by(
        self, column_source: str, column_target: str, value: Union[float, str]
    ):
        """
        Validation the correspondence of a column value based on another column minimum

        Args:
            column_source (str): Column used to obtain the row with the min value
            column_target (str): Column used to verify the matching value
            value (str,number): The value to match against
        """
        (
            Rule(
                "has_min_by",
                [column_source, column_target],
                value,
                RuleDataType.DUO,
            )
            >> self._rule
        )
        return self

    def has_workflow(
        self,
        column_group: str,
        column_event: str,
        column_order: str,
        edges: List[Tuple[str]],
        pct: float = 1.0,
    ):
        """
        Validates events in a group clause with order, followed a specific sequence. Similar to adjacency matrix validation.

        Args:
            column_group (str): The dataframe column used to group events
            column_event (str): The state of the event within the group
            column_order (List[date,number,str]): The order within the group, should be deterministic and without collisions.
            edges (List[Tuple[str,str]]): The combinations of events expected in the data frame i.e `[("A","B"), ("B","C")]`


        ???+ example "Example"

            Given the following fictitious dataset example:

            | date       | ticket   | status      |
            |------------|----------|-------------|
            | 2024-01-01 | CASE-001 | New         |
            | 2024-01-02 | CASE-001 | In Progress |
            | 2024-01-03 | CASE-001 | Closed      |

            You can validate that events for each ticket follow certain sequence by using:

            ``` python
            from cuallee import Check, CheckLevel
            df = spark.createDataFrame(
                 [
                     ["2024-01-01", "CASE-001", "New"],
                     ["2024-01-02", "CASE-001", "In Progress"],
                     ["2024-01-03", "CASE-001", "Closed"],
                 ],
                 ["date", "ticket", "status"],
             )


            check = Check(CheckLevel.WARNING, "WorkflowValidation")
            check.has_workflow(
                column_group="ticket",
                column_event="status",
                column_order="date",
                edges=[(None, "New"),("New", "In Progress"),("In Progress","Closed"), ("Closed", None)]
            )

            # Validate
            check.validate(df).show(truncate=False)

            # Result
            +---+-------------------+------------------+-------+----------------------------+------------+------------------------------------------------------------------------------------+----+----------+---------+--------------+------+
            |id |timestamp          |check             |level  |column                      |rule        |value                                                                               |rows|violations|pass_rate|pass_threshold|status|
            +---+-------------------+------------------+-------+----------------------------+------------+------------------------------------------------------------------------------------+----+----------+---------+--------------+------+
            |1  |2024-05-11 11:24:00|WorkflowValidation|WARNING|('ticket', 'status', 'date')|has_workflow|((None, 'New'), ('New', 'In Progress'), ('In Progress', 'Closed'), ('Closed', None))|3   |0         |1.0      |1.0           |PASS  |
            +---+-------------------+------------------+-------+----------------------------+------------+------------------------------------------------------------------------------------+----+----------+---------+--------------+------+

            ```

        The check validates that:

        - Nothing preceeds a `New` state
        - `In Progress` follows the `New` event
        - `Closed` follows the `In Progress` event
        - Nothing follows after `Closed` state

        """
        (
            Rule(
                "has_workflow",
                [column_group, column_event, column_order],
                edges,
                RuleDataType.AGNOSTIC,
                pct,
            )
            >> self._rule
        )
        return self

    def is_custom(
        self,
        column: Union[str, List[str]],
        fn: Callable = None,
        pct: float = 1.0,
        options: Dict[str, str] = {},
    ):
        """
        Uses a user-defined function that receives the to-be-validated dataframe
        and uses the last column of the transformed dataframe to summarize the check

        Args:
            column (str): Column(s) required for custom function
            fn (Callable): A function that receives a dataframe as input and returns a dataframe with at least 1 column as result
            pct (float): The threshold percentage required to pass
        """

        (
            Rule("is_custom", column, fn, RuleDataType.AGNOSTIC, pct, options=options)
            >> self._rule
        )
        return self
