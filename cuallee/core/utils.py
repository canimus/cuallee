import inspect
from typing import List, Set, Union

from rich import print as rich_print
from toolz import groupby, keyfilter, valmap

from .check import Check
from .rule import Rule, RuleDataType


def get_column_set(columns: Union[str, List[str]]) -> List[str]:
    """Flatten nested column structure into a list of column names"""
    return [col] if isinstance(columns, str) else [c for col in columns for c in (get_column_set(col) if isinstance(col, list) else [col])]


def get_rules(rules: List[Rule], data_type: RuleDataType = None) -> List[Rule]:
    """Get rules optionally filtered by data type"""
    return rules if data_type is None else [rule for rule in rules if rule.data_type == data_type]


def get_rules_by_type(rules: List[Rule], rule_type: RuleDataType) -> List[Rule]:
    """Get rules matching a specific data type"""
    return get_rules(rules, rule_type)


def get_numeric_rules(rules: List[Rule]) -> List[Rule]:
    """Based on a rule list it returns all matching data type: NUMERIC"""
    return get_rules_by_type(rules, RuleDataType.NUMERIC)


def get_date_rules(rules: List[Rule]) -> List[Rule]:
    """Based on a rule list it returns all matching data type: DATE"""
    return get_rules_by_type(rules, RuleDataType.DATE)


def get_timestamp_rules(rules: List[Rule]) -> List[Rule]:
    """Based on a rule list it returns all matching data type: TIMESTAMP"""
    return get_rules_by_type(rules, RuleDataType.TIMESTAMP)


def get_string_rules(rules: List[Rule]) -> List[Rule]:
    """Based on a rule list it returns all matching data type: STRING"""
    return get_rules_by_type(rules, RuleDataType.STRING)


def get_rule_columns(rules: List[Rule]) -> List[str]:
    """Based on a rule list it returns a flatten set of unique columns"""
    return get_column_set([r.column for r in rules])


def match_columns(on_rule: List[Rule], on_dataframe: List[str], case_sensitive: bool = True) -> Set[str]:
    """Check if rule columns exist in dataframe, with optional case sensitivity"""
    rule_cols = set(get_column_set([r.column for r in on_rule]))
    if not case_sensitive:
        return set(map(str.casefold, rule_cols)).difference(map(str.casefold, on_dataframe))
    return rule_cols.difference(on_dataframe)


def inventory():
    """List all available checks in cuallee"""
    methods = dict(inspect.getmembers(Check, predicate=inspect.isfunction))
    method_info = valmap(lambda x: (x.__module__, x.__name__), methods)
    grouped = groupby(lambda x: x[0], method_info.values())
    rich_print(keyfilter(lambda x: "core.check" not in x, grouped))
