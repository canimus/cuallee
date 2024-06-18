import enum
import operator
import pandas as pd  # type: ignore
from dataclasses import dataclass
from typing import Dict, List, Union
from string import Template
from toolz import valfilter  # type: ignore
from google.cloud import bigquery
from cuallee import Check, ComputeEngine, Rule
import logging

logger = logging.getLogger("cuallee")


class ComputeMethod(enum.Enum):
    SQL = "SQL"
    TRANSFORM = "TRANSFORM"


@dataclass
class ComputeInstruction:
    predicate: Union[str, List[str], None]
    expression: str
    compute_method: ComputeMethod

    def __repr__(self):
        return f"ComputeInstruction({self.compute_method})"


class Compute(ComputeEngine):
    def __init__(self, table_name: str = None):
        self.compute_instruction: Union[ComputeInstruction, None] = None

    def _sum_predicate_to_integer(self, predicate) -> str:
        return f"SUM(CAST({predicate} AS INTEGER))".replace(",)", ")")

    def is_complete(self, rule: Rule):
        """Verify the absence of null values in a column"""
        predicate = f"{rule.column} IS NOT NULL"
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.SQL,
        )
        return self.compute_instruction

    def is_empty(self, rule: Rule):
        """Verify the presence of null values in a column"""
        predicate = f"{rule.column} IS NULL"
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.SQL,
        )
        return self.compute_instruction

    def are_complete(self, rule: Rule):
        """Verify the absence of null values in a column"""
        predicate = [f"{c} IS NOT NULL" for c in rule.column]
        self.compute_instruction = ComputeInstruction(
            predicate,
            "("
            + "+".join([self._sum_predicate_to_integer(p) for p in predicate])
            + f")/{len(rule.column)}",
            ComputeMethod.SQL,
        )
        return self.compute_instruction

    def is_unique(self, rule: Rule):
        """Validation for unique values in column"""
        predicate = None
        self.compute_instruction = ComputeInstruction(
            predicate,
            f"COUNT(DISTINCT {rule.column})",
            ComputeMethod.SQL,
        )
        return self.compute_instruction

    def has_cardinality(self, rule: Rule):
        """Validation of number of distinct values in column"""
        self.compute_instruction = ComputeInstruction(
            None,
            f"COUNT(DISTINCT({rule.column}))={rule.value}",
            ComputeMethod.SQL,
        )
        return self.compute_instruction

    def has_infogain(self, rule: Rule):
        """More than 1 distinct value"""
        self.compute_instruction = ComputeInstruction(
            None,
            f"COUNT(DISTINCT({rule.column})) > 1",
            ComputeMethod.SQL,
        )
        return self.compute_instruction

    def are_unique(self, rule: Rule):
        """Validation for unique values in a group of columns"""
        predicate = None
        self.compute_instruction = ComputeInstruction(
            predicate,
            "COUNT(DISTINCT CONCAT("
            + ", '_', ".join([f"{c}" for c in rule.column])
            + "))",
            ComputeMethod.SQL,
        )
        return self.compute_instruction

    def is_contained_in(self, rule: Rule):
        """Validation of column value in set of given values"""

        predicate = f"{rule.column} IN {rule.value}"
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.SQL,
        )
        return self.compute_instruction

    def not_contained_in(self, rule: Rule):
        """Validation of column value not in a set of given values"""
        predicate = f"{rule.column} NOT IN {rule.value}"
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.SQL,
        )
        return self.compute_instruction

    def is_daily(self, rule: Rule):
        """Validates that there is no missing dates using only week days in the date/timestamp column"""

        predicate = None

        def _execute(dataframe: bigquery.table.Table, key: str) -> str:
            day_mask = rule.value
            if not day_mask:
                day_mask = tuple([2, 3, 4, 5, 6])

            script = Template(
                """SELECT CASE WHEN numb_rows > 0 THEN CAST(numb_rows*-1 AS STRING) ELSE "true" END AS KEY$key FROM (SELECT COUNT (*) AS numb_rows FROM(SELECT full_interval.date FROM (SELECT date FROM (SELECT date, EXTRACT(DAYOFWEEK FROM date) AS dayofweek FROM UNNEST((SELECT GENERATE_DATE_ARRAY(min, max) AS date FROM (SELECT CAST(MIN($column) AS DATE) AS min, CAST(MAX($column) AS DATE)AS max FROM $table))) AS date ORDER BY date) WHERE dayofweek IN $value) AS full_interval LEFT OUTER JOIN $table AS table ON (full_interval.date=CAST(table.$column AS DATE)) WHERE $column IS NULL))""".strip()
            )

            return script.substitute(
                table=dataframe, key=key, column=rule.column, value=day_mask
            )

        self.compute_instruction = ComputeInstruction(
            predicate,
            _execute,
            ComputeMethod.TRANSFORM,
        )
        return self.compute_instruction


def _get_expressions(compute_set: Dict[str, ComputeInstruction]) -> str:
    """Get the expression for all the rules in check in one string"""

    return ", ".join(
        [
            compute_instruction.expression + f" AS KEY{key}"
            for key, compute_instruction in compute_set.items()
        ]
    )


def _build_query(expression_string: str, dataframe: bigquery.table.Table) -> str:
    """Build query final query"""

    return f"SELECT {expression_string} FROM `{str(dataframe)}`"


def _compute_query_method(
    client, dataframe: bigquery.table.Table, compute_set: Dict[str, ComputeInstruction]
) -> Dict:
    """Compute rules throught query"""

    # Filter expression directed to sql
    _sql = lambda x: x.compute_method.name == ComputeMethod.SQL.name
    sql_set = valfilter(_sql, compute_set)

    if sql_set:
        expression_string = _get_expressions(sql_set)
        query = _build_query(expression_string, dataframe)
        logger.error(query)

        return client.query(query).to_arrow().to_pandas().to_dict(orient="records")[0]
    else:
        return {}


def _compute_transform_method(
    client, dataframe: bigquery.table.Table, compute_set: Dict[str, ComputeInstruction]
) -> Dict:
    """Compute rules that require to pass the table as variable"""

    # Filter expression directed to transform
    _transform = lambda x: x.compute_method.name == ComputeMethod.TRANSFORM.name
    transform_set = valfilter(_transform, compute_set)

    return {
        f"KEY{k}": operator.itemgetter(f"KEY{k}")(
            client.query(compute_instruction.expression(dataframe, k))
            .to_arrow()
            .to_pandas()
            .to_dict(orient="records")[0]
        )
        for k, compute_instruction in transform_set.items()
    }


def _compute_row(client, dataframe: bigquery.table.Table) -> Dict:
    """Get the number of rows"""

    return (
        client.query(f"SELECT COUNT(*) AS count FROM `{str(dataframe)}`")
        .to_arrow()
        .to_pandas()
        .to_dict(orient="records")
    )


def _calculate_violations(result, nrows) -> Union[int, float]:
    """Return the number of violations for each rule"""

    if (result == "true") or (result is True):
        return 0
    elif (result == "false") or (result is False):
        return nrows
    elif int(result) < 0:
        return abs(int(result))
    else:
        return nrows - int(result)


def _calculate_pass_rate(result, nrows) -> float:
    """Return the pass rate for each rule"""

    if (result == "true") or (result is True):
        return 1.0
    elif (result == "false") or (result is False):
        return 0.0
    elif int(result) < 0:
        if abs(int(result)) < nrows:
            return 1 - (abs(int(result)) / nrows)
        elif abs(int(result)) == nrows:
            return 0.5
        else:
            return nrows / abs(int(result))
    else:
        return int(result) / nrows


def _evaluate_status(pass_rate, pass_threshold) -> str:
    """Return the status for each rule"""

    if pass_rate >= pass_threshold:
        return "PASS"
    else:
        return "FAIL"


def validate_data_types(rules: List[Rule], dataframe: str) -> bool:
    """Validate the datatype of each column according to the CheckDataType of the rule's method"""
    return True


def compute(rules: Dict[str, Rule]) -> Dict:
    """Create dictionnary containing compute instruction for each rule."""
    return {k: operator.methodcaller(v.method, v)(Compute()) for k, v in rules.items()}


def summary(check: Check, dataframe: bigquery.table.Table):
    """Compute all rules in this check from table loaded in BigQuery"""

    # Check that user is connected to BigQuery
    try:
        client = bigquery.Client()
    except Exception as error:
        print(
            f"You are not connected to the BigQuery cloud. Please verify the steps followed during the Authenticate API requests step. {str(error)}"
        )

    # Compute the expression
    computed_expressions = compute(check._rule)

    query_expression = _compute_query_method(client, dataframe, computed_expressions)
    query_transform = _compute_transform_method(client, dataframe, computed_expressions)
    query_result = {**query_expression, **query_transform}

    # Compute the total number of rows
    rows = _compute_row(client, dataframe)[0]["count"]

    # Results
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
            "violations": _calculate_violations(query_result[f"KEY{hash_key}"], rows),
            "pass_rate": _calculate_pass_rate(query_result[f"KEY{hash_key}"], rows),
            "pass_threshold": rule.coverage,
            "status": _evaluate_status(
                _calculate_pass_rate(query_result[f"KEY{hash_key}"], rows),
                rule.coverage,
            ),
        }
        for index, (hash_key, rule) in enumerate(check._rule.items(), 1)
    ]

    return pd.DataFrame(computation_basis).set_index("id")
