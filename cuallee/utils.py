from typing import Set, Union, List
from cuallee import Rule, CheckDataType


def get_column_set(columns: Union[str, List[str]]) -> List[str]:
    """Confirm that all compute columns exists in dataframe"""

    def _normalize_columns(col: Union[str, List[str]], agg: List[str]) -> List[str]:
        """Recursive consilidation of compute columns"""
        if isinstance(col, str):
            agg.append(col)
        else:
            [_normalize_columns(inner_col, agg) for inner_col in col]
        return agg

    return _normalize_columns(columns, [])


def get_numeric_rules(rules: List[Rule]) -> List[Rule]:
    """Based on a rule list it returns all matching data type: NUMERIC"""
    return list(filter(lambda x: x.data_type.name == CheckDataType.NUMERIC.name, rules))


def get_date_rules(rules: List[Rule]) -> List[Rule]:
    """Based on a rule list it returns all matching data type: DATE"""
    return list(filter(lambda x: x.data_type.name == CheckDataType.DATE.name, rules))


def get_timestamp_rules(rules: List[Rule]) -> List[Rule]:
    """Based on a rule list it returns all matching data type: TIMESTAMP"""
    return list(
        filter(lambda x: x.data_type.name == CheckDataType.TIMESTAMP.name, rules)
    )


def get_string_rules(rules: List[Rule]) -> List[Rule]:
    """Based on a rule list it returns all matching data type: STRING"""
    return list(filter(lambda x: x.data_type.name == CheckDataType.STRING.name, rules))


def get_rule_columns(rules: List[Rule]) -> List[str]:
    """Based on a rule list it returns a flatten set of unique columns"""
    return get_column_set(list(map(lambda x: x.column, rules)))  # type: ignore


def match_data_types(on_rule: List[str], on_dataframe: List[str]) -> Set[str]:
    """Compare sets between rule and dataframe"""
    return set(on_rule).difference(on_dataframe)


def match_columns(
    on_rule: List[Rule], on_dataframe: List[str], case_sensitive: bool = True
) -> Set[str]:
    """Confirms all columns in check exists in dataframe"""
    dataframe_columns = on_dataframe
    rule_columns = set(get_rule_columns(on_rule))
    if not case_sensitive:
        rule_columns = map(str.casefold, rule_columns)
        dataframe_columns = map(str.casefold, on_dataframe)

    return set(rule_columns).difference(dataframe_columns)
