import operator
import textwrap
from functools import reduce
from numbers import Number

import duckdb as dk
import numpy as np
import pandas as pd  # type: ignore
from pygments import highlight  # type: ignore
from pygments.formatters import TerminalTrueColorFormatter  # type: ignore
from pygments.lexers import SqlLexer  # type: ignore
from toolz import first  # type: ignore

from cuallee import Check, Rule


class Compute:
    def is_complete(self, rule: Rule) -> str:
        """Verify the absence of null values in a column"""
        return f"SUM(CAST({rule.column} IS NOT NULL AS INTEGER))"

    def are_complete(self, rule: Rule) -> str:
        """Verify the abscence of null values on groups of columns"""
        return f"SUM( " + " + ".join([f"(CAST({column} IS NOT NULL AS INTEGER))" for column in rule.column]) + f") / {float(len(rule.column))}"

    def is_unique(self, rule: Rule) -> str:
        """Confirms the absence of duplicate values in a column"""
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

    def has_sum(self, rule: Rule) -> str:
        return f"SUM({rule.column}) = {rule.value}"

    def is_between(self, rule: Rule) -> str:
        return f"CAST({rule.column} BETWEEN '{rule.value[0]}' AND '{rule.value[1]}' AS INTEGER)"

    def is_contained_in(self, rule: Rule) -> str:
        return f"CAST({rule.column} IN {rule.value} AS INTEGER)"

    def has_percentile(self, rule: Rule) -> str:
        return (
            f"APPROX_QUANTILE({rule.id}, {rule.value}) = {rule.settings['percentile']}"
        )

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
        return (
            f"CAST(EXTRACT('hour' from {rule.column}) BETWEEN {rule.value} AS INTEGER)"
        )

    def is_daily(self, rule: Rule) -> str:
        raise NotImplementedError("😔 Sorry, still working on this feature.")

    def is_inside_interquartile_range(self, rule: Rule) -> str:
        raise NotImplementedError("😔 Sorry, still working on this feature.")

    def has_workflow(self, rule: Rule) -> str:
        raise NotImplementedError("😔 Sorry, still working on this feature.")


def validate_data_types(check: Check, dataframe: dk.DuckDBPyConnection):
    return True


def compute(check: Check):
    return True


def summary(check: Check, connection: dk.DuckDBPyConnection) -> list:

    unified_columns = ",\n\t".join(
        [
            operator.methodcaller(rule.method, rule)(Compute()) + f" AS '{rule.key}'"
            for rule in check.rules
        ]
    )
    unified_query = f"""
    SELECT
    \t{unified_columns}
    FROM
    \t{check.table_name}
    """

    print(
        highlight(
            textwrap.dedent(unified_query), SqlLexer(), TerminalTrueColorFormatter()
        )
    )

    def _calculate_violations(result, nrows):
        if isinstance(result, (bool, np.bool_)):
            if result:
                return 0
            else:
                return nrows
        elif isinstance(result, Number):
            if isinstance(result, complex):
                return result.imag
            else:
                return nrows - result

    def _calculate_pass_rate(result, nrows):

        if isinstance(result, (bool, np.bool_)):
            if result:
                return 1.0
            else:
                return 0.0
        elif isinstance(result, Number):
            if isinstance(result, complex):
                if result.imag > 0:
                    return nrows / result.imag
                else:
                    return 1.0
            else:
                return result / nrows

    def _evaluate_status(pass_rate, pass_threshold):

        if pass_rate >= pass_threshold:
            return "PASS"
        else:
            return "FAIL"

    _merge_dicts = lambda a, b: {**a, **b}
    unified_results = reduce(
        _merge_dicts, connection.execute(unified_query).df().to_dict(orient="records")
    )

    rows = first(
        connection.execute(f"select count(*) from {check.table_name}").fetchone()
    )
    computation_basis = [
        {
            "id": index,
            "timestamp": check.date.strftime("%Y-%m-%d %H:%M:%S"),
            "check": check.name,
            "level": check.level.name,
            "column": rule.column,
            "rule": rule.method,
            "value": rule.value,
            "rows": rows,
            "violations": _calculate_violations(unified_results[hash_key], rows),
            "pass_rate": _calculate_pass_rate(unified_results[hash_key], rows),
            "pass_threshold": rule.coverage,
            "status": _evaluate_status(
                _calculate_pass_rate(unified_results[hash_key], rows),
                rule.coverage,
            ),
        }
        for index, (hash_key, rule) in enumerate(check._rule.items(), 1)
    ]
    return pd.DataFrame(computation_basis).reset_index(drop=True)
