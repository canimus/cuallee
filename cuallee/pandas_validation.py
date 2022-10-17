from typing import Dict
from cuallee import Check, Rule
import pandas as pd  # type: ignore
import operator


class Compute:
    def is_complete(self, rule: Rule):
        def _execute(dataframe: pd.DataFrame):
            return dataframe.loc[:, rule.column].isnull().sum()

        return _execute


def compute(rules: Dict[str, Rule]):
    return {k: operator.methodcaller(v.method, v)(Compute()) for k, v in rules.items()}


def validate_data_types(rules: Dict[str, Rule], dataframe: pd.DataFrame):
    return True


def summary(check: Check, dataframe: pd.DataFrame):
    computation = compute(check._rule)
    return pd.DataFrame(
        {rule.method: [computation[rule.key](dataframe)] for rule in check.rules}
    )
