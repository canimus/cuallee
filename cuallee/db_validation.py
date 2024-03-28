import operator
import numpy as np
import pandas as pd
import polars as pl

from toolz import first
from numbers import Number
from functools import reduce

from cuallee import Check, db_connector
from cuallee.duckdb_validation import Compute as duckdb_compute


class Compute(duckdb_compute):

    def __init__(self, table_name: str = None):
        super().__init__(table_name)

def validate_data_types(check: Check, dataframe):
    return True

def compute(check: Check):
    return True


def summary(check: Check, connection: db_connector) -> list:
    unified_columns = ",\n\t".join(
        [
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

    # print(
    #     highlight(
    #         textwrap.dedent(unified_query), SqlLexer(), TerminalTrueColorFormatter()
    #     )
    # )

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

    #NOTE: identifier "B58F8BBF3BEFEBCE45F552AD29CC697673FB82A6D64F30C1F2AB3435E96D5431" will
    #NOTE: be truncated to "B58F8BBF3BEFEBCE45F552AD29CC697673FB82A6D64F30C1F2AB3435E96D543"

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