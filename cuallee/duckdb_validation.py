from typing import Dict, Union, List
from cuallee import Check, Rule
import duckdb as dk
import operator
import numpy as np
import re
from toolz import first  # type: ignore
from numbers import Number
from cuallee import utils as cuallee_utils


class Compute:
    def is_complete(
        self, rule: Rule
    ) -> Union[bool, int]:
        return f"sum(cast({rule.column} IS NOT NULL AS INTEGER)) as '{rule.key}'"

    def is_unique(
        self, rule: Rule
    ) -> Union[bool, int]:
        return f"count(distinct({rule.column})) as '{rule.key}'"

    def are_complete(
        self, rule: Rule
    ) -> Union[bool, int]:
        return f"sum(cast({rule.column} IS NOT NULL AS INTEGER)) / { len(rule.column) } as '{rule.key}'"

    def is_unique(
        self, rule: Rule
    ) -> Union[bool, int]:
        return f"count(distinct({rule.column})) as '{rule.key}'"
        
def validate_data_types(check: Check, dataframe: dk.DuckDBPyConnection):
    return True

def compute(check: Check):
    return True

def summary(check: Check, connection: dk.DuckDBPyConnection) -> list:

    unified_columns = ",\n".join([operator.methodcaller(rule.method, rule)(Compute()) for rule in check.rules])
    unified_query = f"""
    SELECT
    {unified_columns}
    FROM
    {check.table_name}
    """

    return connection.execute(unified_query).fetchall()