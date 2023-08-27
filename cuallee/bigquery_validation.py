import enum
import operator
import pandas as pd
from dataclasses import dataclass
from typing import Dict, List, Union
from google.cloud import bigquery
from cuallee import Check, ComputeEngine, Rule


class ComputeMethod(enum.Enum):
    SQL = "SQL"


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
        return f"SUM(CAST({predicate} AS INTEGER))"

    def is_complete(self, rule: Rule):
        """Verify the absence of null values in a column"""
        predicate = f"{rule.column} IS NOT NULL"
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

    return f"SELECT {expression_string} FROM {dataframe}"


def _compute_query_method(client, query: str) -> Dict:
    """Compute rules throught query"""

    return client.query(query).to_arrow().to_pandas().to_dict(orient="records")


def _compute_row(client, dataframe: bigquery.table.Table) -> Dict:
    """Get the number of rows"""

    return (
        client.query(f"SELECT COUNT(*) AS count FROM {dataframe}")
        .to_arrow()
        .to_pandas()
        .to_dict(orient="records")
    )


def _calculate_violations(result, nrows) -> Union[int, float]:
    """Return the number of violations for each rule"""

    if isinstance(result, bool):
        if result:
            return 0
        else:
            return nrows
    else:
        if result < 0:
            return abs(result)
        else:
            return nrows - result


def _calculate_pass_rate(result, nrows) -> float:
    """Return the pass rate for each rule"""

    if isinstance(result, bool):
        if result:
            return 1.0
        else:
            return 0.0
    elif result < 0:
        if abs(result) < nrows:
            return 1 - (abs(result) / nrows)
        elif abs(result) == nrows:
            return 0.5
        else:
            return nrows / abs(result)
    else:
        return result / nrows


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
    except:
        print(
            "You are not connected to the BigQuery cloud. Please verify the steps followed during the Authenticate API requests step."
        )

    # Compute the expression
    computed_expressions = compute(check._rule)

    expression_string = _get_expressions(computed_expressions)
    query = _build_query(expression_string, dataframe)
    query_result = _compute_query_method(client, query)[0]

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
