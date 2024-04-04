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

    def has_max(self, rule: Rule) -> str:
        """Validation of a column’s maximum value"""
        return f"IF(MAX({rule.column}) = {rule.value}, 'True', 'False')"

    def has_min(self, rule: Rule) -> str:
        """Validation of a column’s minimum value"""
        return f"IF(MIN({rule.column}) = {rule.value}, 'True', 'False')"

    def has_std(self, rule: Rule) -> str:
        """Validate standard deviation on column.
        Issues: #Note: This could fail due to floating point precision
        Suggestions: #IDEA: Use f"CAST(STDDEV_SAMP({rule.column}) AS FLOAT) - CAST({rule.value} AS FLOAT) < {percision_error}"
        """

        return f"IF( CAST(STDDEV_SAMP({rule.column}) AS FLOAT) = CAST({rule.value} AS FLOAT), 'True', 'False')"

    def has_mean(self, rule: Rule) -> str:
        """Validation of a column's average/mean"""
        return f"IF(AVG({rule.column}) = {rule.value}, 'True', 'False')"

    def has_sum(self, rule: Rule) -> str:
        """Validation of a column’s sum of values"""
        return f"IF(SUM({rule.column}) = {rule.value}, 'True', 'False')"

    def has_infogain(self, rule: Rule) -> str:
        """Validation column with more than 1 value"""
        return f"IF(COUNT(DISTINCT({rule.column})) > 1, 'True', 'False')"

    def has_cardinality(self, rule: Rule) -> str:
        """Validation of a column’s different values"""
        return f"IF(COUNT(DISTINCT({rule.column})) = {rule.value}, 'True', 'False')"

    def has_percentile(self, rule: Rule) -> str:
        """Validation of a column percentile value"""
        return f"QUANTILE_CONT({rule.column}, {rule.settings['percentile']}) = {rule.value}"

    def has_pattern(self, rule: Rule) -> str:
        """Validation for string type column matching regex expression"""
        return f"SUM(CAST(REGEXP_MATCHES({rule.column}, '{rule.value}') AS INTEGER))"

    def is_between(self, rule: Rule) -> str:
        """Validation of a column between a range"""
        return f"SUM({rule.column} BETWEEN '{rule.value[0]}' AND '{rule.value[1]}')"

    def has_correlation(self, rule: Rule) -> str:
        """Validates the correlation between 2 columns with some tolerance"""
        raise NotImplementedError

    def has_entropy(self, rule: Rule) -> str:
        """Computes entropy of 0-1 vector."""
        raise NotImplementedError

    def is_complete(self, rule: Rule) -> str:
        """Verify the absence of null values in a column"""
        return f"SUM({rule.column} IS NOT NULL)"

    def are_complete(self, rule: Rule) -> str:
        """Verify the abscence of null values on groups of columns"""
        return (
            "SUM( "
            + " + ".join(
                [f"({column} IS NOT NULL)" for column in rule.column]
            )
            + f") / {float(len(rule.column))}"
        )

    def is_greater_than(self, rule: Rule) -> str:
        """Validation for numeric greater than value"""
        return f"SUM({rule.column} > {rule.value})"

    def is_less_than(self, rule: Rule) -> str:
        """Validation for numeric less than value"""
        return f"SUM({rule.column} < {rule.value})"

    def is_greater_or_equal_than(self, rule: Rule) -> str:
        """Validation for numeric greater or equal than value"""
        return f"SUM({rule.column} >= {rule.value})"

    def is_less_or_equal_than(self, rule: Rule) -> str:
        """Validation for numeric less or equal than value"""
        return f"SUM({rule.column} <= {rule.value})"

    def is_equal_than(self, rule: Rule) -> str:
        """Validation for numeric column equal than value"""
        return f"SUM({rule.column} = {rule.value})"

    def is_contained_in(self, rule: Rule) -> str:
        return f"SUM({rule.column} IN {rule.value})"

    def not_contained_in(self, rule: Rule) -> str:
        """Validation of column value not in a set of given values"""
        return f"SUM({rule.column} NOT IN {rule.value})"

    def satisfies(self, rule: Rule) -> str:
        return f"SUM({rule.value})"

    def is_on_weekday(self, rule: Rule) -> str:
        """Validates a datetime column is in a Mon-Fri time range"""
        return f"SUM(DAYOFWEEK({rule.column}) BETWEEN 2 AND 6)"

    def is_on_weekend(self, rule: Rule) -> str:
        """Validates a datetime column is in a Sat-Sun time range"""
        return f"SUM(DAYOFWEEK({rule.column}) IN (1,7))"

    def is_on_monday(self, rule: Rule) -> str:
        """Validates a datetime column is on Mon"""
        return f"SUM(DAYOFWEEK({rule.column}) = 2 )"

    def is_on_tuesday(self, rule: Rule) -> str:
        """Validates a datetime column is on Tue"""
        return f"SUM(DAYOFWEEK({rule.column}) = 3 )"

    def is_on_wednesday(self, rule: Rule) -> str:
        """Validates a datetime column is on Wed"""
        return f"SUM(DAYOFWEEK({rule.column}) = 4 )"

    def is_on_thursday(self, rule: Rule) -> str:
        """Validates a datetime column is on Thu"""
        return f"SUM(DAYOFWEEK({rule.column}) = 5 )"

    def is_on_friday(self, rule: Rule) -> str:
        """Validates a datetime column is on Fri"""
        return f"SUM(DAYOFWEEK({rule.column}) = 6 )"

    def is_on_saturday(self, rule: Rule) -> str:
        """Validates a datetime column is on Sat"""
        return f"SUM(DAYOFWEEK({rule.column}) = 7 )"

    def is_on_sunday(self, rule: Rule) -> str:
        """Validates a datetime column is on Sun"""
        return f"SUM(DAYOFWEEK({rule.column}) = 1 )"

    def is_on_schedule(self, rule: Rule) -> str:
        """Validation of a datetime column between an hour interval"""
        return f"SUM(HOUR({rule.column}) BETWEEN {rule.value[0]} AND {rule.value[1]})"

    def has_pattern(self, rule: Rule) -> str:  # noqa: F811
        """Validation for string type column matching regex expression"""
        return f"SUM({rule.column} REGEXP '{rule.value}')"

    def is_inside_interquartile_range(self, rule: Rule) -> str:
        """Validates a number resides inside the Q3 - Q1 range of values"""
        raise NotImplementedError

    def are_unique(self, rule: Rule) -> str:
        """Validate absence of duplicate in group of columns"""
        return "( "+ " + ".join( f"COUNT(DISTINCT({column}))" for column in rule.column) + f" ) / {float(len(rule.column))} "

    def has_entropy(self, rule: Rule) -> str: # noqa: F811
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

    def has_percentile(self, rule: Rule) -> str: # noqa: F811
        """Percentile range verification for column"""
        raise NotImplementedError

    def is_inside_interquartile_range(self, rule: Rule) -> str: # noqa: F811
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
        elif isinstance(result, str):
            if result.lower() == "true":
                return 0
            elif result.lower() == "false":
                return nrows

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
        elif isinstance(result, str):
            if result.lower() == 'true':
                return 1.0
            elif result.lower() == 'false':
                return 0.0

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
            "violations": _calculate_violations(first(unified_results[hash_key]), rows),
            "pass_rate": _calculate_pass_rate(first(unified_results[hash_key]), rows),
            "pass_threshold": rule.coverage,
            "status": _evaluate_status(
                _calculate_pass_rate(first(unified_results[hash_key]), rows),
                rule.coverage,
            ),
        }
        for index, (hash_key, rule) in enumerate(check._rule.items(), 1)
    ]
    pl.Config.set_tbl_cols(12)
    return pl.DataFrame(computation_basis)