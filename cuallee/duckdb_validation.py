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
    def is_complete(self, rule: Rule) -> str:
        return f"SUM(CAST({rule.column} IS NOT NULL AS INTEGER))"

    def are_complete(self, rule: Rule) -> str:
        return f"SUM(CAST({rule.column} IS NOT NULL AS INTEGER)) / { len(rule.column) }"

    def is_unique(self, rule: Rule) -> str:
        return f"COUNT(DISTINCT({rule.column}))"

    def are_unique(self, rule: Rule) -> str:
        return f"COUNT(DISTINCT({rule.column})) / {len(rule.column)}"

    def is_greater_than(self, rule: Rule) -> str:
        return f"CAST({rule.column} > {rule.value} AS INTEGER)"

    def is_less_than(self, rule: Rule) -> str:
        return f"CAST({rule.column} < {rule.value} AS INTEGER)"

    def is_grester_or_equal_than(self, rule: Rule) -> str:
        return f"CAST({rule.column} >= {rule.value} AS INTEGER)"

    def is_less_or_equal_than(self, rule: Rule) -> str:
        return f"CAST({rule.column} <= {rule.value} AS INTEGER)"
    
    def is_equal_than(self, rule: Rule) -> str:
        return f"CAST({rule.column} = {rule.value} AS INTEGER)"

    def has_pattern(self, rule: Rule) -> str:
        return f"CAST(REGEXP_MATCHES({rule.column}, '{rule.value}') AS INTEGER)"

    def has_min(self, rule: Rule) -> str:
        return f"MIN({rule.column}) = {rule.value}"

    def has_max(self, rule: Rule) -> str:
        return f"MAX({rule.column}) = {rule.value}"
    
    def has_std(self, rule: Rule) -> str:
        return f"STDDEV_POP({rule.column}) = {rule.value}"
    
    def has_mean(self, rule: Rule) -> str:
        return f"AVG({rule.column}) = {rule.value}"

    def is_between(self, rule: Rule) -> str:
        return f"CAST({rule.column} BETWEEN '{rule.value[0]}' AND '{rule.value[1]}' AS INTEGER)"

    def is_contained_in(self, rule: Rule) -> str:
        return f"CAST({rule.column} IN {rule.value} AS INTEGER)"

    def has_percentile(self, rule: Rule) -> str:
        return f"APPROX_QUANTILE({rule.id}, {rule.value[0]}) = {rule.value[1]}"

    def has_max_by(self, rule: Rule) -> str:
        return f"MAX_BY({rule.column[1]}, {rule.column[0]}) = {rule.value}"
    
    def has_min_by(self, rule: Rule) -> str:
        return f"MIN_BY({rule.column[1]}, {rule.column[0]}) = {rule.value}"
    
    def has_correlation(self, rule: Rule) -> str:
        return f"CORR({rule.columns[0]}, {rule.column[1]}) = {rule.value}"

    def satisfies(self, rule: Rule) -> str:
        return f"CAST(({rule.value}) AS INTEGER)"

    def has_entropy(self, rule: Rule) -> str:
        return f"ENTROPY({rule.column}) = {rule.value}"

    def is_on_weekday(self, rule: Rule) -> str:
        return f"CAST(EXTRACT('dayofweek' from {rule.column}) BETWEEN (1,5) AS INTEGER)"

    def is_on_weekend(self, rule: Rule) -> str:
        return f"CAST(EXTRACT('dayofweek' from {rule.column}) IN (0,6) AS INTEGER)"

    def is_on_monday(self, rule: Rule) -> str:
        return f"CAST(EXTRACT('dayofweek' from {rule.column}) = 1 AS INTEGER)"

    def is_on_tuesday(self, rule: Rule) -> str:
        return f"CAST(EXTRACT('dayofweek' from {rule.column}) = 2 AS INTEGER)"

    def is_on_wednesday(self, rule: Rule) -> str:
        return f"CAST(EXTRACT('dayofweek' from {rule.column}) = 3 AS INTEGER)"

    def is_on_thursday(self, rule: Rule) -> str:
        return f"CAST(EXTRACT('dayofweek' from {rule.column}) = 4 AS INTEGER)"

    def is_on_friday(self, rule: Rule) -> str:
        return f"CAST(EXTRACT('dayofweek' from {rule.column}) = 5 AS INTEGER)"
    
    def is_on_saturday(self, rule: Rule) -> str:
        return f"CAST(EXTRACT('dayofweek' from {rule.column}) = 6 AS INTEGER)"

    def is_on_sunday(self, rule: Rule) -> str:
        return f"CAST(EXTRACT('dayofweek' from {rule.column}) = 0 AS INTEGER)"

    def is_on_schedule(self, rule: Rule) -> str:
        return f"CAST(EXTRACT('hour' from {rule.column}) BETWEEN {rule.value} AS INTEGER)"

    def is_daily(self, rule: Rule) -> str:
        pass
    
    


def validate_data_types(check: Check, dataframe: dk.DuckDBPyConnection):
    return True


def compute(check: Check):
    return True


def summary(check: Check, connection: dk.DuckDBPyConnection) -> list:

    unified_columns = ",\n".join(
        [operator.methodcaller(rule.method, rule)(Compute()) for rule in check.rules]
    )
    unified_query = f"""
    SELECT
    {unified_columns}
    FROM
    {check.table_name}
    """
    print(unified_query)

    return connection.execute(unified_query).fetchall()
