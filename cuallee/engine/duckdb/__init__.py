import operator
import textwrap
from functools import reduce
from typing import Any, List, Union

import duckdb as dk
import pandas as pd  # type: ignore
from toolz import first  # type: ignore

from ...core.check import Check
from ...core.rule import Rule
from . import predicates as P


def dtypes(rules: List[Rule], dataframe: Union[dk.DuckDBPyConnection, Any]):
    return True


def summary(check: Check, connection: dk.DuckDBPyConnection) -> list:
    if isinstance(connection, dk.DuckDBPyRelation):
        raise NotImplementedError(
            textwrap.dedent(
                """
        Invalid DuckDb object, please pass a DuckDbPyConnection instead. And register your relation like:
        conn = duckdb.connect()
        check = Check(table_name="demo_table")
        duckdb_relation_object = conn.sql("FROM range(10)")
        conn.register(view_name=check.table_name, python_object=duckdb_relation_object)
        check.is_complete("range").validate(conn)
        """
            )
        )

    unified_columns = ",\n\t".join(
        [
            operator.methodcaller(rule.method, rule)(P) + f" AS '{rule.key}'"
            for rule in check.rules
        ]
    )
    unified_query = f"""
    SELECT
    \t{unified_columns}
    FROM
    \t'{check.table_name}'
    """

    print(unified_query)

    _merge_dicts = lambda a, b: {**a, **b}
    unified_results = reduce(
        _merge_dicts, connection.execute(unified_query).df().to_dict(orient="records")
    )
    rows = first(
        connection.execute(f"select count(*) from '{check.table_name}'").fetchone()
    )

    for index, (hash_key, rule) in enumerate(check._rule.items(), 1):
        rule.ordinal = index
        rule.evaluate(unified_results[hash_key], rows)

    computation_basis = [
        {
            "id": rule.ordinal,
            "timestamp": check.date.strftime("%Y-%m-%d %H:%M:%S"),
            "check": check.name,
            "level": check.level.name,
            "column": rule.column,
            "rule": rule.name,
            "value": rule.format_value(),
            "rows": rows,
            "violations": rule.violations,
            "pass_rate": rule.pass_rate,
            "pass_threshold": rule.coverage,
            "status": rule.status,
        }
        for rule in check.rules
    ]
    return pd.DataFrame(computation_basis).reset_index(drop=True)
