from typing import Union, List

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

