import operator
from functools import reduce
from numbers import Number

import duckdb as dk
import numpy as np
import pandas as pd  # type: ignore
from toolz import first  # type: ignore
from string import Template

from cuallee import Check, Rule


class Compute:
    def __init__(self, table_name: str = None):
        self.table_name = table_name

    def is_complete(self, rule: Rule) -> str:
        """Verify the absence of null values in a column"""
        return f"SUM(CAST({rule.column} IS NOT NULL AS INTEGER))"

    def is_empty(self, rule: Rule) -> str:
        """Verify the presence of null values in a column"""
        return f"SUM(CAST({rule.column} IS NULL AS INTEGER))"

    def are_complete(self, rule: Rule) -> str:
        """Verify the abscence of null values on groups of columns"""
        return (
            "SUM( "
            + " + ".join(
                [f"(CAST({column} IS NOT NULL AS INTEGER))" for column in rule.column]
            )
            + f") / {float(len(rule.column))}"
        )

    def is_unique(self, rule: Rule) -> str:
        """Confirms the absence of duplicate values in a column"""
        return f"COUNT(DISTINCT({rule.column}))"

    def are_unique(self, rule: Rule) -> str:
        return (
            "( "
            + " + ".join([f"approx_count_distinct({column})" for column in rule.column])
            + f") / cast({float(len(rule.column))} AS FLOAT)"
        )

    def is_greater_than(self, rule: Rule) -> str:
        return f"SUM(CAST({rule.column} > {rule.value} AS INTEGER))"

    def is_less_than(self, rule: Rule) -> str:
        return f"SUM(CAST({rule.column} < {rule.value} AS INTEGER))"

    def is_greater_or_equal_than(self, rule: Rule) -> str:
        return f"SUM(CAST({rule.column} >= {rule.value} AS INTEGER))"

    def is_less_or_equal_than(self, rule: Rule) -> str:
        return f"SUM(CAST({rule.column} <= {rule.value} AS INTEGER))"

    def is_equal_than(self, rule: Rule) -> str:
        return f"SUM(CAST({rule.column} = {rule.value} AS INTEGER))"

    def has_pattern(self, rule: Rule) -> str:
        return f"SUM(CAST(REGEXP_MATCHES({rule.column}, '{rule.value}') AS INTEGER))"

    def has_min(self, rule: Rule) -> str:
        return f"MIN({rule.column}) = {rule.value}"

    def has_max(self, rule: Rule) -> str:
        return f"MAX({rule.column}) = {rule.value}"

    def has_std(self, rule: Rule) -> str:
        return f"STDDEV_SAMP({rule.column}) = {rule.value}"

    def has_mean(self, rule: Rule) -> str:
        return f"AVG({rule.column}) = {rule.value}"

    def has_sum(self, rule: Rule) -> str:
        return f"SUM({rule.column}) = {rule.value}"

    def has_cardinality(self, rule: Rule) -> str:
        return f"COUNT(DISTINCT({rule.column})) = {rule.value}"

    def has_infogain(self, rule: Rule) -> str:
        return f"COUNT(DISTINCT({rule.column})) > 1"

    def is_between(self, rule: Rule) -> str:
        return f"SUM(CAST({rule.column} BETWEEN '{rule.value[0]}' AND '{rule.value[1]}' AS INTEGER))"

    def is_contained_in(self, rule: Rule) -> str:
        return f"SUM(CAST({rule.column} IN {rule.value} AS INTEGER))"

    def not_contained_in(self, rule: Rule) -> str:
        """Validation of column value not in a set of given values"""
        return f"SUM(CAST({rule.column} NOT IN {rule.value} AS INTEGER))"

    def has_percentile(self, rule: Rule) -> str:
        return f"QUANTILE_CONT({rule.column}, {rule.settings['percentile']}) = {rule.value}"

    def has_max_by(self, rule: Rule) -> str:
        return f"MAX_BY({rule.column[0]}, {rule.column[1]}) = '{rule.value}'"

    def has_min_by(self, rule: Rule) -> str:
        return f"MIN_BY({rule.column[0]}, {rule.column[1]}) = '{rule.value}'"

    def has_correlation(self, rule: Rule) -> str:
        return f"CORR({rule.column[0]}, {rule.column[1]}) = {rule.value}"

    def satisfies(self, rule: Rule) -> str:
        return f"SUM(CAST(({rule.value}) AS INTEGER))"

    def has_entropy(self, rule: Rule) -> str:
        return f"ENTROPY({rule.column}) = {rule.value}"

    def is_on_weekday(self, rule: Rule) -> str:
        return f"SUM(CAST(EXTRACT(dow from {rule.column}) BETWEEN 1 AND 5 AS INTEGER))"

    def is_on_weekend(self, rule: Rule) -> str:
        return f"SUM(CAST(EXTRACT(dow from {rule.column}) IN (0,6) AS INTEGER))"

    def is_on_monday(self, rule: Rule) -> str:
        return f"SUM(CAST(EXTRACT(dow from {rule.column}) = 1 AS INTEGER))"

    def is_on_tuesday(self, rule: Rule) -> str:
        return f"SUM(CAST(EXTRACT(dow from {rule.column}) = 2 AS INTEGER))"

    def is_on_wednesday(self, rule: Rule) -> str:
        return f"SUM(CAST(EXTRACT(dow from {rule.column}) = 3 AS INTEGER))"

    def is_on_thursday(self, rule: Rule) -> str:
        return f"SUM(CAST(EXTRACT(dow from {rule.column}) = 4 AS INTEGER))"

    def is_on_friday(self, rule: Rule) -> str:
        return f"SUM(CAST(EXTRACT(dow from {rule.column}) = 5 AS INTEGER))"

    def is_on_saturday(self, rule: Rule) -> str:
        return f"SUM(CAST(EXTRACT(dow from {rule.column}) = 6 AS INTEGER))"

    def is_on_sunday(self, rule: Rule) -> str:
        return f"SUM(CAST(EXTRACT(dow from {rule.column}) = 0 AS INTEGER))"

    def is_on_schedule(self, rule: Rule) -> str:
        return f"SUM(CAST(EXTRACT(hour from {rule.column}) BETWEEN {rule.value[0]} AND {rule.value[1]} AS INTEGER))"

    def is_daily(self, rule: Rule) -> str:
        """Returns the number or violations and matches on a daily schedule"""

        if rule.value is None:
            day_mask = tuple([1, 2, 3, 4, 5])
        else:
            day_mask = rule.value

        template = Template(
            """
            distinct(select LIST_VALUE(count(B.$id),SUM(CAST(B.$id IS NULL AS INTEGER))::INTEGER) as r from (
            select distinct(unnest(range(min($id)::TIMESTAMP, cast(date_add(max($id), INTERVAL 1 DAY) as TIMESTAMP), INTERVAL 1 DAY))) as w, 
            extract(dow from w) as y from '$table'
            ) A LEFT JOIN '$table' B ON A.w = B.$id where A.y in $value)
        """.strip()
        )

        return template.substitute(
            {"id": rule.column, "value": str(day_mask), "table": self.table_name}
        )

    def is_inside_interquartile_range(self, rule: Rule) -> str:
        template = Template(
            """
            (select SUM(CAST(A.$id BETWEEN B.q[1] AND B.q[2] AS INTEGER)) as r from $table A,(
            select QUANTILE_CONT($id, [0.25, 0.75]) as q from $table) B)
        """.strip()
        )
        return template.substitute({"id": rule.column, "table": self.table_name})

    def has_workflow(self, rule: Rule) -> str:
        template = Template(
            """
        (select sum(A.CUALLEE_RESULT) from (
            select
            lead($event) over (partition by $name order by $ordinal) as CUALLEE_EVENT,
            LIST_VALUE($event, CUALLEE_EVENT) as CUALLEE_EDGE,
            LIST_VALUE$basis as CUALLEE_GRAPH,
            CAST(array_has(CUALLEE_GRAPH, CUALLEE_EDGE) AS INTEGER) as CUALLEE_RESULT
            from '$table'
        ) as A)
        """.strip()
        )
        name, event, ordinal = rule.column
        basis = str(tuple(map(list, rule.value))).replace("None", "NULL")
        return template.substitute(
            {
                "name": name,
                "event": event,
                "ordinal": ordinal,
                "basis": basis,
                "table": self.table_name,
            }
        )


def validate_data_types(check: Check, dataframe: dk.DuckDBPyConnection):
    return True


def compute(check: Check):
    return True


def summary(check: Check, connection: dk.DuckDBPyConnection) -> list:
    unified_columns = ",\n\t".join(
        [
            operator.methodcaller(rule.method, rule)(Compute(check.table_name))
            + f" AS '{rule.key}'"
            for rule in check.rules
        ]
    )
    unified_query = f"""
    SELECT
    \t{unified_columns}
    FROM
    \t'{check.table_name}'
    """

    def _calculate_violations(result, nrows):
        if isinstance(result, (bool, np.bool_)):
            if result:
                return 0
            else:
                return nrows
        elif isinstance(result, Number):
            return nrows - result
        elif isinstance(result, (list, np.ndarray)):
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
        elif isinstance(result, (list, np.ndarray)):
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

    _merge_dicts = lambda a, b: {**a, **b}
    unified_results = reduce(
        _merge_dicts, connection.execute(unified_query).df().to_dict(orient="records")
    )

    rows = first(
        connection.execute(f"select count(*) from '{check.table_name}'").fetchone()
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
