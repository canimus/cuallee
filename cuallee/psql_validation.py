import logging
import operator
import numpy as np
import polars as pl

from toolz import first
from numbers import Number

from cuallee import Check, Rule, db_connector
from cuallee.duckdb_validation import Compute as duckdb_compute

import textwrap
from pygments import highlight
from pygments.lexers import SqlLexer
from pygments.formatters.terminal256 import TerminalTrueColorFormatter


class Compute(duckdb_compute):

    def __init__(self, table_name: str = None):
        super().__init__(table_name)

    def are_unique(self, rule: Rule) -> str:
        """Validate absence of duplicate in group of columns"""
        return "( "+ " + ".join( f"COUNT(DISTINCT({column}))" for column in rule.column) + f" ) / {float(len(rule.column))} "

    def has_std(self, rule: Rule) -> str:
        """Validate standard deviation on column.
        Issues: #Note: This could fail due to floating point precision
        Suggestions: #IDEA: Use f"CAST(STDDEV_SAMP({rule.column}) AS FLOAT) - CAST({rule.value} AS FLOAT) < {percision_error}"
        """

        return f"CAST(STDDEV_SAMP({rule.column}) AS FLOAT) = CAST({rule.value} AS FLOAT)"

    def has_pattern(self, rule: Rule) -> str:
        """Validation for string type column matching regex expression"""
        return f"SUM(CASE WHEN {rule.column} ~ '{rule.value}' THEN 1 ELSE 0 END)"

    def has_entropy(self, rule: Rule) -> str:
        """Computes entropy of 0-1 vector."""
        raise NotImplementedError

    def has_max_by(self, rule: Rule) -> str:
        """Adjacent column maximum value verifiation on threshold"""
        """
        ```sql
        SELECT id
        FROM public.test1
        WHERE id2 = (SELECT MAX(id2) FROM public.test1);
        ```
        """
        raise NotImplementedError

    def has_min_by(self, rule: Rule) -> str:
        """Adjacent column minimum value verifiation on threshold"""
        """
        ```sql
        SELECT id
        FROM public.test1
        WHERE id2 = (SELECT MIN(id2) FROM public.test1);
        ```
        """
        raise NotImplementedError

    def has_percentile(self, rule: Rule) -> str:
        """Percentile range verification for column"""
        return f"PERCENTILE_CONT({rule.settings['percentile']}) WITHIN GROUP (ORDER BY {rule.column})  = {rule.value}"

    def is_inside_interquartile_range(self, rule: Rule) -> str:
        """Validates a number resides inside the Q3 - Q1 range of values"""
        return f"""
                SUM(CASE WHEN {rule.column}
                BETWEEN
                (SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {rule.column}) AS first_quartile FROM {self.table_name})
                AND
                (SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {rule.column}) AS third_quartile FROM {self.table_name})
                THEN 1 ELSE 0 END)
                """

    def has_workflow(self, rule: Rule) -> str:
        """Validates events in a group clause with order, followed a specific sequence. Similar to adjacency matrix validation"""
        raise NotImplementedError

def validate_data_types(check: Check, dataframe):
    return True

def compute(check: Check):
    return True


def summary(check: Check, connection: db_connector) -> list:
    """
    Note:
    -----
    It appears that the hash key's length exceeds Postgres' limits, resulting in it being truncated.

    For example, the original hash key "B58F8BBF3BEFEBCE45F552AD29CC697673FB82A6D64F30C1F2AB3435E96D5431"
    will be shortened to "B58F8BBF3BEFEBCE45F552AD29CC697673FB82A6D64F30C1F2AB3435E96D543".

    To prevent the side effects of this truncation, the last character is removed from the hash key by using: `hash_key[:-1]`
    """

    unified_columns = ",\n\t".join(
        [
            # This is the same as compute.`rule.method`(rule)
            operator.methodcaller(rule.method, rule)(Compute(check.table_name))
            + f' AS "{rule.key}"'
            for rule in check.rules
        ]
    )
    unified_query = f"""
    SELECT
    \t{unified_columns}
    FROM
    \t{check.table_name}
    """

    logging.debug( highlight( textwrap.dedent(unified_query), SqlLexer(), TerminalTrueColorFormatter() ) )

    def _calculate_violations(result, nrows):
        if isinstance(result, (bool, np.bool_)):
            if result:
                return 0
            else:
                return nrows
        elif isinstance(result, Number):
            return nrows - result
        elif isinstance(result, list):
            if len(result) == 2:
                return result[1]

    def _calculate_pass_rate(result, nrows):
        if isinstance(result, (bool, np.bool_)):
            if result:
                return 1.0
            else:
                return 0.0
        elif isinstance(result, Number):
            return result / nrows
        elif isinstance(result, list):
            if result[1] > 0:
                if result[1] > nrows:
                    return nrows / result[1]
                else:
                    return result[1] / nrows
            else:
                return 1.0

    def _evaluate_status(pass_rate, pass_threshold):
        if pass_rate >= pass_threshold:
            return "PASS"
        else:
            return "FAIL"

    rows = connection(query = f"select count(*) from {check.table_name}").item(0,0)

    unified_results = connection(query = unified_query).to_dict(as_series=False)

    computation_basis = [
        {
            "id": index,
            "timestamp": check.date.strftime("%Y-%m-%d %H:%M:%S"),
            "check": check.name,
            "level": check.level.name,
            "column": str(rule.column),
            "rule": rule.method,
            "value": str(rule.value),
            "rows": rows,
            "violations": _calculate_violations(first(unified_results[hash_key[:-1]]), rows),
            "pass_rate": _calculate_pass_rate(first(unified_results[hash_key[:-1]]), rows),
            "pass_threshold": rule.coverage,
            "status": _evaluate_status(
                _calculate_pass_rate(first(unified_results[hash_key[:-1]]), rows),
                rule.coverage,
            ),
        }
        for index, (hash_key, rule) in enumerate(check._rule.items(), 1)
    ]
    pl.Config.set_tbl_cols(12)
    return pl.DataFrame(computation_basis)