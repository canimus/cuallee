from typing import Dict
from cuallee import Check, Rule
import pandas as pd  # type: ignore
import operator
from toolz import first
from numbers import Number


class Compute:
    def is_complete(self, rule: Rule, dataframe: pd.DataFrame) -> pd.DataFrame:
        return dataframe.loc[:, rule.column].notnull().sum()


def compute(rules: Dict[str, Rule]):
    pass


def validate_data_types(rules: Dict[str, Rule], dataframe: pd.DataFrame):
    return True


def summary(check: Check, dataframe: pd.DataFrame):
    compute = Compute()
    unified_results = {
        rule.key: [operator.methodcaller(rule.method, rule, dataframe)(compute)]
        for rule in check.rules
    }

    def _calculate_violations(result, nrows):
        if isinstance(result, bool):
            if result:
                return 0
            else:
                return nrows
        elif isinstance(result, Number):
            return nrows - result

    def _calculate_pass_rate(result, nrows):
        if isinstance(result, bool):
            if result:
                return 1.0
            else:
                return 0
        elif isinstance(result, Number):
            return result / nrows

    def _evaluate_status(pass_rate, pass_threshold):
        if pass_rate >= pass_threshold:
            return "PASS"
        else:
            return "FAIL"

    rows = len(dataframe)
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
    return pd.DataFrame(computation_basis)
